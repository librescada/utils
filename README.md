This repository holds the project's common dependencies.

The idea is to configure a CI, that whenever a new tagged commit is pushed, builds a new package librescada_utils, that can be imported from the rest of the modules.

During development, this repository should also be included in the other's.



### Setting up the environment

In order to execute any of the modules here, first you need to install the dependencies. The easiest way to do this is to use the provided `requirements.txt` file, it includes the installation of the librescada_utils module in editable mode, which is a common dependency for the project and is included in this repository as a submodule.

```bash
pip install -r requirements.txt
```