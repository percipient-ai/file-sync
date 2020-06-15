from setuptools import setup, find_packages

setup(
    name="file_sync",
    version="1.0",
    description="Mirage™ File Sync",
    url="https://www.percipient.ai",
    author="percipient.ai",
    license="Proprietary",
    include_package_data=True,
    packages=find_packages(),
    package_data={
        "file_sync": [
            "config/logging_config.ini",
        ]
    },
    entry_points={"console_scripts": ["file_sync=file_sync.agent:main"],
                  "file_sync_plugins": ["s3 = plugins.s3filesync.file_sync:do_run"]},
)
