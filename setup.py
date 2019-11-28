from setuptools import setup, find_packages

setup(
    name='slurppy',
    version='0.0.1',
    url='https://github.com/christian-oreilly/slurppy.git',
    author="Christian O'Reilly",
    author_email='christian.oreilly@gmail.com',
    description='Light-weight package to manage computationally-intensive processing pipelines using SLURM.',
    packages=find_packages(),    
    install_requires=["matplotlib", "numpy", "pyyaml", "jinja2", "blockdiag", "pytest"],
)
