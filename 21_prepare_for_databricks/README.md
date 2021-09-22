# Prepare for Databricks

Let us take care of setting up the project for Databricks. Some of these steps are covered earlier as part of setting up local development environment to build data engineering pipelines for Databricks.

* Make sure existing Pyspark is uninstalled.

```shell script
pip uninstall pyspark==\*
```

* Install Databricks Connect as part of the virtual environment. Our original Pyspark version is 2.4.x, but as part of the Databricks we will be using Spark 3.0.1. In case of larger applications we might run into compatibility issues.

```shell script
pip install -U databricks-connect==7.3.\*
```

* Make sure to have a development cluster as explained as part of previous section.
* You can use existing configuration or refresh the configuration. 
* Make sure to get the token from Databricks Console in case if you are configuring Databricks Connect for the first time.
* Also, configure local Databricks Connect to development cluster as demonstrated in previous section.

```shell script
databricks-connect configure
databricks-connect test
```

* Copy the code and confirm that there are no errors. 
* For now comment out read, process and write in **app.py**.

```python
def main():


if __name__ == '__main__':
    main()
```
* We will refactor the code for Databricks as part of the subsequent topics in this section or module.
