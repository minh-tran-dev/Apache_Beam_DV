import setuptools

REQUIRED_PACKAGES = [
    "apache-beam",
    "apache-beam[gcp]",
    "apache_beam[dataframe]",
    "google",
    "google-cloud-storage",
    "google-cloud-bigquery",
    "google-cloud-logging",
    "pydantic",
    "pandas",
]

setuptools.setup(
    name="FG-APACHE-BEAM",
    version="0.1.0",
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
    include_package_data=True,
)
