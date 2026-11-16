from enum import Enum
class Role(str, Enum):
    admin = 'admin'
    analyst = 'analyst'
    viewer = 'viewer'
class JobStatus(str, Enum):
    queued = 'queued'
    running = 'running'
    completed = 'completed'
    failed = 'failed'
    paused = 'paused'

class CheckpointStatus(str, Enum):
    creating = 'creating'
    ready = 'ready'
    failed = 'failed'

class RestoreStatus(str, Enum):
    pending = 'pending'
    running = 'running'
    finalizing = 'finalizing'
    completed = 'completed'
    failed = 'failed'
