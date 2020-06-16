from setuptools import setup

setup(
    name="s3filesync",
    version="1.0",
    description="Mirage™ S3 File Sync Plugin",
    url="https://www.percipient.ai",
    author="percipient.ai",
    license="Proprietary",
    entry_points={"file_sync_plugins": ["s3=file_sync.plugins.s3filesync.file_sync:do_run"]},
)
