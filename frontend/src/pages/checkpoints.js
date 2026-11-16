import {checkpoints} from '../api/entities.js';
import {workspace} from '../stores/workspace.js';
import {session} from '../stores/session.js';
import {heading} from '../components/common/shell.js';
import {esc,date,notify} from '../utils/dom.js';
import {submit} from '../hooks/forms.js';

const CHECKPOINT_STATUS = {creating: '取点中', ready: '就绪', failed: '取点失败'};
const RESTORE_STATUS = {pending: '等待开始', running: '恢复中', finalizing: '收尾中', completed: '已完成', failed: '恢复失败'};
const STEP_LABEL = {
  queued: '排队', quiescing: '等待在途操作收口', snapshotting: '固定数据与归档文件',
  verifying: '校验清单与指纹', staging: '暂存归档文件', staged: '归档文件已暂存',
  'backing-up': '备份当前归档', 'switching-files': '切换归档文件',
  'switched-files': '归档文件已切换', 'switching-records': '切换数据记录',
  'switched-records': '数据记录已切换', finalizing: '收尾',
  ready: '就绪', completed: '完成', failed: '失败',
};
const MODE_LABEL = {normal: '服务正常', quiescing: '取点准备', capturing: '取点中', restoring: '恢复中'};

let timer = null;
let renderRecall = () => {};

export async function checkpointPage() {
  const status = await checkpoints.status();
  workspace.checkpointStatus = status;
  const {state, items, operation, counts} = status;
  const active = state.mode !== 'normal' || items.some(c => c.status === 'creating') ||
    (operation && ['pending', 'running', 'finalizing'].includes(operation.status));
  if (timer) clearTimeout(timer);
  if (active) timer = setTimeout(() => {
    timer = null;
    // 只在仍停留在本页时自动刷新，避免打断其他页面操作。
    if (location.hash.replace(/^#\//, '').split('/')[0] === 'checkpoints') renderRecall();
  }, 2000);

  return heading('06 / RECOVERY', '整站检查点与停机恢复',
    '取点会先暂停业务写入与派发，等待在途操作收口后固定同一时点的数据记录与归档文件。负责人可预检并恢复；缺失文件或指纹不符会被拒绝。',
    '<button class="button" id="checkpoint-refresh">刷新进度 ↻</button>') +
    stateBanner(state, operation) +
    '<div class="checkpoint-grid"><section class="panel"><div class="section-title"><h2>创建整站检查点</h2><span>仅负责人</span></div>' +
    '<form id="checkpoint-form"><label>检查点名称<input name="name" required minlength="2" maxlength="160" placeholder="例如：11 月停机演练前"></label>' +
    '<button type="submit" class="primary" ' + (session.user.role !== 'admin' ? 'disabled' : '') + '>暂停写入并取点</button></form>' +
    '<p class="hint">取点期间业务写入与派发会短暂暂停；新建的分析任务与分发计划会被拒绝。当前数据：' +
    counts.datasets + ' 个批次 / ' + counts.jobs + ' 个分析 / ' + counts.reports + ' 个分发计划。</p>' +
    '<div class="state-facts"><div><span>整站状态</span><strong>' + esc(MODE_LABEL[state.mode] || state.mode) + '</strong></div>' +
    '<div><span>恢复栅栏 epoch</span><strong>' + state.epoch + '</strong></div>' +
    '<div><span>更新时间</span><strong>' + date(state.updated_at) + '</strong></div></div></section>' +
    '<section class="panel"><div class="section-title"><h2>恢复操作</h2><span>最近一次</span></div>' +
    (operation ? operationDetail(operation) : '<p class="empty">尚未执行恢复。检查点就绪后先预检，确认记录与文件齐全再恢复。</p>') +
    '</section></div>' +
    '<section class="panel"><div class="section-title"><h2>检查点清单</h2><span>' + items.length + ' 个</span></div>' +
    checkpointList(items) + '</section>';
}

function stateBanner(state, operation) {
  if (state.mode === 'normal' && (!operation || operation.status !== 'failed')) return '';
  if (state.mode !== 'normal') {
    return '<div class="transfer-banner" role="status"><strong>' + esc(MODE_LABEL[state.mode]) + '</strong><span>' +
      esc(state.message || '业务写入与派发暂停中，请等待完成。') + '</span></div>';
  }
  return '<div class="transfer-banner failed" role="alert"><strong>最近一次恢复失败</strong><span>' +
    esc(operation.error || '未知原因') + '；系统已保留旧版本，可修正后重试同一检查点。</span></div>';
}

function operationDetail(op) {
  const done = op.status === 'completed';
  const failed = op.status === 'failed';
  return '<article class="restore-detail ' + esc(op.status) + '">' +
    '<div class="restore-head"><strong>' + esc(RESTORE_STATUS[op.status] || op.status) + '</strong>' +
    '<span>操作 #' + op.id + ' · 检查点 #' + op.checkpoint_id + ' · epoch ' + op.epoch + ' · 第 ' + op.attempts + ' 次</span></div>' +
    '<div class="progress-track"><i style="width:' + Math.max(0, Math.min(100, op.progress || 0)) + '%"></i></div>' +
    '<p class="restore-step">当前步骤：' + esc(STEP_LABEL[op.step] || op.step) + ' · ' + (op.progress || 0) + '%</p>' +
    (op.error ? '<p class="warning">失败原因：' + esc(op.error) + '</p>' : '') +
    '<p class="hint">' + (done ? '恢复已完成。恢复出的分发计划已暂停，需负责人逐条确认后再启用。' :
      failed ? '记录与文件已回滚到旧版本；重试同一检查点会复用此操作，不会重复出刊。' :
        '恢复期间业务读写与派发一律拒绝，旧执行结果与旧队列消息不会复活已回退的任务。') + '</p>' +
    '<p class="hint">开始 ' + date(op.created_at) + (op.finished_at ? ' · 结束 ' + date(op.finished_at) : '') + '</p></article>';
}

function checkpointList(items) {
  if (!items.length) return '<div class="empty">尚无检查点。创建后会在这里显示取点进度、数据批次、分析、计划与归档文件的数量及指纹。</div>';
  return '<div class="checkpoint-list">' + items.map(cp => {
    const counts = cp.counts || {};
    const restore = cp.restore;
    return '<article class="checkpoint-item ' + esc(cp.status) + '">' +
      '<div class="checkpoint-head"><div><strong>' + esc(cp.name) + '</strong>' +
      '<p>' + esc(CHECKPOINT_STATUS[cp.status] || cp.status) + ' · ' + date(cp.created_at) +
      (cp.taken_at ? ' · 时点 ' + date(cp.taken_at) : '') + ' · ' + esc(cp.created_by) + '</p></div>' +
      '<div class="checkpoint-actions">' +
      '<button class="button small" data-preflight="' + cp.id + '" ' + (cp.status !== 'ready' || session.user.role !== 'admin' ? 'disabled' : '') + '>预检</button>' +
      '<button class="button small primary" data-restore="' + cp.id + '" ' +
      (cp.status !== 'ready' || session.user.role !== 'admin' || (restore && ['pending', 'running', 'finalizing'].includes(restore.status)) ? 'disabled' : '') + '>' +
      (restore && restore.status === 'failed' ? '重试恢复' : '恢复到此检查点') + '</button></div></div>' +
      '<div class="checkpoint-facts"><span>数据批次 ' + (counts.datasets || 0) + '</span>' +
      '<span>分析 ' + (counts.jobs || 0) + '</span><span>计划 ' + (counts.reports || 0) + '</span>' +
      '<span>归档文件 ' + (cp.artifacts || []).length + '</span>' +
      '<span>内容指纹 ' + esc((cp.data_checksum || '').slice(0, 12)) + '…</span></div>' +
      '<p class="checkpoint-progress">' + (cp.status === 'creating' ? '取点进度 ' + (cp.progress || 0) + '% · ' + esc(STEP_LABEL[cp.step] || cp.step) : '') + '</p>' +
      (cp.error ? '<p class="warning">取点失败：' + esc(cp.error) + '</p>' : '') +
      (cp.preflight_at ? '<p class="checkpoint-preflight ' + (cp.preflight_ok ? 'ok' : 'bad') + '">预检 ' +
        (cp.preflight_ok ? '通过' : '未通过') + ' · ' + date(cp.preflight_at) + ' · ' + esc(cp.preflight_actor) +
        (!cp.preflight_ok && cp.preflight_detail?.problems?.length ? '：' + esc(cp.preflight_detail.problems.slice(0, 3).join('；')) : '') + '</p>' : '') +
      (restore ? '<p class="checkpoint-restore">恢复 ' + esc(RESTORE_STATUS[restore.status] || restore.status) +
        ' · ' + (restore.progress || 0) + '% · ' + esc(STEP_LABEL[restore.step] || restore.step) +
        (restore.error ? ' · ' + esc(restore.error) : '') + '</p>' : '') +
      '</article>';
  }).join('') + '</div>';
}

export function bindCheckpoints(refresh) {
  renderRecall = refresh;
  if (timer) clearTimeout(timer);
  timer = null;
  document.querySelector('#checkpoint-refresh').onclick = refresh;
  const form = document.querySelector('#checkpoint-form');
  if (form) submit(form, async f => {
    const cp = await checkpoints.create({name: f.get('name')});
    notify('已开始取点：正在等待在途操作收口');
    workspace.checkpointStatus = await checkpoints.status();
    await refresh();
    return cp;
  });
  document.querySelectorAll('[data-preflight]').forEach(b => b.onclick = async () => {
    try {
      const result = await checkpoints.preflight(b.dataset.preflight);
      notify(result.ok ? '预检通过：记录与归档文件齐全，指纹一致' : '预检未通过：' + result.problems.slice(0, 2).join('；'), !result.ok);
      await refresh();
    } catch (error) { notify(error.message, true); }
  });
  document.querySelectorAll('[data-restore]').forEach(b => b.onclick = async () => {
    if (!confirm('将拒绝业务读写并回退到该检查点，恢复出的分发计划会先暂停。继续？')) return;
    try {
      b.disabled = true;
      await checkpoints.restore(b.dataset.restore);
      notify('恢复已开始：业务读写与派发暂停');
      await refresh();
    } catch (error) { notify(error.message, true); b.disabled = false; }
  });
}
