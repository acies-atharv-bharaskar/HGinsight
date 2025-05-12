from setuptools import setup, find_packages

setup(
    name="s3-postgres-pipeline",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "boto3>=1.26.0",
        "pandas>=1.5.0",
        "pyarrow>=11.0.0",
        "psycopg2-binary>=2.9.5",
        "sqlalchemy>=2.0.0",
        "sentence-transformers>=2.2.2",
        "tqdm>=4.65.0",
        "numpy>=1.23.0",
    ],
    entry_points={
        "console_scripts": [
            "run-pipeline=scripts.run_pipeline:main",
            "test-connection=scripts.test_connection:main",
        ],
    },
    python_requires=">=3.8",
    author="Your Name",
    author_email="your.email@example.com",
    description="Pipeline for importing S3 Parquet data to PostgreSQL with search enhancements",
    keywords="s3, postgresql, pipeline, embeddings, full-text search",
    url="https://github.com/yourusername/s3-postgres-pipeline",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Database :: Database Engines/Servers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)