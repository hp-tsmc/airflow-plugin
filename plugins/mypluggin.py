from airflow.plugins_manager import AirflowPlugin
from airflow.listeners import hookimpl


class MyAirflowPlugin(AirflowPlugin):
    # name your plugin, this is mandatory
    name = "myplug"

    ## Add plugin components
    # ...
    # ...
    # ...

    # Add an optional callback to perform actions when airflow starts and
    # the plugin is loaded.
    # NOTE: Ensure your plugin has *args, and **kwargs in the method definition
    #   to protect against extra parameters injected into the on_load()
    #   function in future changes
    def on_load(*args, **kwargs):
        # perform Plugin boot actions
        pass

    from airflow.listeners import hookimpl
    from airflow.models.dagrun import DagRun










    # [END howto_listen_ti_failure_task]

    # [START howto_listen_dagrun_success_task]
    @hookimpl
    def on_dag_run_success(dag_run: DagRun, msg: str):
        """
        This method is called when dag run state changes to SUCCESS.
        """
        print("Dag run in success state")
        start_date = dag_run.start_date
        end_date = dag_run.end_date

        print(f"Dag run start:{start_date} end:{end_date}")



    # [END howto_listen_dagrun_success_task]

    # [START howto_listen_dagrun_failure_task]
    @hookimpl
    def on_dag_run_failed(dag_run: DagRun, msg: str):
        """
        This method is called when dag run state changes to FAILED.
        """
        print("Dag run  in failure state")
        dag_id = dag_run.dag_id
        run_id = dag_run.run_id
        external_trigger = dag_run.external_trigger

        print(f"Dag information:{dag_id} Run id: {run_id} external trigger: {external_trigger}")



    # [END howto_listen_dagrun_failure_task]

    # [START howto_listen_dagrun_running_task]
    @hookimpl
    def on_dag_run_running(dag_run: DagRun, msg: str):
        """
        This method is called when dag run state changes to RUNNING.
        """
        print("Dag run  in running state")
        queued_at = dag_run.queued_at
        dag_hash_info = dag_run.dag_hash

        print(f"Dag information Queued at: {queued_at} hash info: {dag_hash_info}")
        # creating a file
        fileObject = open("/opt/airflow/logs/gfg.txt", "w+")
        
        # writing into the file
        fileObject.write("Geeks 4 geeks !")
        
        # closing the file
        fileObject.close()



    # [END howto_listen_dagrun_running_task]