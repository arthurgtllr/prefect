from prefect.runner import serve
from prefect.deployments.runner import RunnerDeployment
import asyncio


async def main():
    await serve(
        await RunnerDeployment.from_remote(
            url="https://github.com/desertaxle/demo.git",
            entrypoint="flow.py:my_flow",
            name="test-remote",
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
