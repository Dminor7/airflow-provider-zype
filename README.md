<p align="center">
  <a href="https://www.airflow.apache.org">
    <img alt="Airflow" src="https://cwiki.apache.org/confluence/download/attachments/145723561/airflow_transparent.png?api=v2" width="60" />
  </a>
</p>
<h1 align="center">
  Airflow Zype Provider
</h1>
  

<br/>




## Functional Testing Standards

To build your repo into a python wheel that can be tested, follow the steps below:

1. Clone the provider repo.
2. `cd` into provider directory.
3. Run `python3 -m pip install build`.
4. Run `python3 -m build` to build the wheel.
5. Find the .whl file in `/dist/*.whl`.
6. Download the [Astro CLI](https://github.com/astronomer/astro-cli).
7. Create a new project directory, cd into it, and run `astro dev init` to initialize a new astro project.
8. Ensure the Dockerfile contains the Airflow 2.0 image:

   ```
   FROM quay.io/astronomer/ap-airflow:2.0.0-buster-onbuild
   ```

9. Copy the `.whl` file to the top level of your project directory.
10. Install `.whl` in your containerized environment by adding the following to your Dockerfile:

   ```
   RUN pip install --user airflow_provider_zype-0.0.1-py3-none-any.whl
   ```

11. Copy example DAG to the `dags/` folder of your astro project directory.
12. Run `astro dev start` to build the containers and run Airflow locally (you'll need Docker on your machine).
13. When you're done, run `astro dev stop` to wind down the deployment. Run `astro dev kill` to kill the containers and remove the local Docker volume. You can also use `astro dev kill` to stop the environment before rebuilding with a new `.whl` file.

> Note: If you are having trouble accessing the Airflow webserver locally, there could be a bug in your wheel setup. To debug, run `docker ps`, grab the container ID of the scheduler, and run `docker logs <scheduler-container-id>` to inspect the logs.

<!-- Once you have built and tested your provider package as a Python wheel, you're ready to [send us your repo](https://registry.astronomer.io/publish-provider) to be published on [The Astronomer Registry](https://registry.astronomer.io). -->
