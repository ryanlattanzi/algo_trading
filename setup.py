import setuptools

with open("requirements.txt") as f:
    requirements = [req for req in f.read().splitlines() if "==" in req]

setuptools.setup(
    name="algo_trading",
    author="Algo Trading Degenerates",
    description="Algo Trading python library",
    url="https://github.com/ryanlattanzi/algo_trading/tree/main/algo_trading",
    version="0.1.0",  # major.minor.micro
    packages=[
        "algo_trading",
        "algo_trading.config",
        "algo_trading.logger",
        "algo_trading.repositories",
        "algo_trading.strategies",
        "algo_trading.utils",
    ],
    install_requires=requirements,
    setup_requires=[
        "wheel",
    ],
)
