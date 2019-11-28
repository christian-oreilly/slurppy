from setuptools import setup, find_packages

setup(
    name='slurppy',
    version='0.0.1',
    license='bsd-3-clause',
    url='https://github.com/christian-oreilly/slurppy.git',
    download_url="https://github.com/christian-oreilly/slurppy/archive/v0.0.1.tar.gz",
    author="Christian O'Reilly",
    author_email='christian.oreilly@gmail.com',
    description='Light-weight package to manage computationally-intensive processing pipelines using SLURM.',
    packages=find_packages(),    
    install_requires=["matplotlib", "numpy", "pyyaml", "jinja2", "blockdiag", "pytest"],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',  # Define that your audience are developers
        'Topic :: Software Development',
        'License :: OSI Approved',  # Again, pick a license
        'Programming Language :: Python :: 3',  # Specify which pyhton versions that you want to support
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'
    ],
)

