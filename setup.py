from setuptools import setup, find_packages

setup(
    name="dagster_pipeline",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "dagster>=1.8.0",
        "dagster-webserver>=1.8.0",
        "dagster-postgres>=0.24.0",
        "pandas>=2.2.0",
        "pyarrow>=17.0.0",
        "boto3>=1.35.0",
        "s3fs>=2024.10.0",
    ],
    extras_require={
        "dev": [
            "pytest>=8.3.0",
            "pytest-mock>=3.14.0",
        ]
    },
    author="Your Name",
    author_email="your.email@example.com",
    description="DataFusion + Dagster example integration",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/datafusion-dagster-example",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
)
