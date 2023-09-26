from pathlib import Path
from typing import Optional, Protocol, runtime_checkable
from urllib.parse import urlparse

from prefect.logging.loggers import get_logger


@runtime_checkable
class Mount(Protocol):
    """
    A mount is a location that can be mounted to a flow runner.
    """

    @property
    def destination(self) -> Path:
        """
        The destination path for the mount.
        """
        ...

    @destination.setter
    def destination(self, destination: Path):
        """
        The destination path for the mount.
        """
        ...

    @property
    def name(self) -> str:
        """
        The name of the mount.
        """
        ...

    async def sync(self):
        """
        Syncs the mount to the local filesystem.
        """
        ...


class GitRepositoryMount:
    """
    Syncs a git repository to the local filesystem.
    """

    def __init__(
        self,
        repository: str,
        name: Optional[str] = None,
        branch: str = "main",
    ):
        self._repository = repository
        self._branch = branch
        self._name = name or urlparse(repository).path.split("/")[-1].replace(
            ".git", ""
        )
        self._logger = get_logger("git-synchronizer")
        self._destination = Path.cwd() / self._name

    @property
    def destination(self) -> Path:
        return self._destination

    @destination.setter
    def destination(self, destination: Path):
        self._destination = destination

    @property
    def name(self) -> str:
        return self._name

    async def sync(self):
        """
        Syncs the repository to the local filesystem.
        """
        self._logger.info(f"Syncing repository to {self.destination}...")
        from anyio import run_process

        git_dir = self.destination / ".git"

        if git_dir.exists():
            # Check if the existing repository matches the configured repository
            result = await run_process(
                ["git", "config", "--get", "remote.origin.url"],
                cwd=str(self.destination),
            )
            existing_repo_url = None
            if result.stdout is not None:
                existing_repo_url = result.stdout.decode().strip()

            if existing_repo_url != self._repository:
                raise ValueError(
                    f"The existing repository at {str(self.destination)} "
                    f"does not match the configured repository {self._repository}"
                )

            # Update the existing repository
            await run_process(
                ["git", "pull", "origin", self._branch], cwd=self.destination
            )
        else:
            # Clone the repository if it doesn't exist at the destination
            result = await run_process(
                [
                    "git",
                    "clone",
                    "--branch",
                    self._branch,
                    self._repository,
                    str(self.destination),
                ]
            )
