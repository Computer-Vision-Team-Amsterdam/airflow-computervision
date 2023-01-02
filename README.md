# Airflow-computervision

This is the airflow repository of the computer vision team.
This repository contains the pipelines that are used in our container project.

We created the pipelines in this repository based on the below image in the [CVT Drafts Miro board](https://miro.com/app/board/uXjVOVQfTW4=/?share_link_id=412250854483).

As you can see in the image below our pipeline is split in three main parts;
<img src="docs/images/miro-pipeline-overview.png">


1. DAG 1. Processing.

<img src="docs/images/miro-pipeline-dag-1.png" width="800">

This pipeline is responsible for retrieving images from cloud, storing them in our storage account from Azure 
and running 2 AI models, the blurring model and the container detection model. 
It also contains tasks to remove the images from the storage account once they're processed. 
This first pipeline is going to be run multiple times in order to optimise the large amount of data that must be processed.


2. DAG 2. Postprocessing.

<img src="docs/images/miro-pipeline-dag-2.png" width="800">

This pipeline contains only one task end is responsible for combining the predictions of the container model with 
two other sources namely the permit data from Decos and the locations of vulnerable bridges. 
This pipeline also creates the maps in HTML format which summarise the current situation of where illegal containers 
are present in the city centre.


3. DAG 3. Submit notifications.

<img src="docs/images/miro-pipeline-dag-3.png" width="600">

This pipeline is responsible for sending a signal to see you based on the content of the maps produced by the previous pipeline. 

### How to trigger a DAG.

Each DAG is triggered manually from airflow. When we trigger one pipeline we trigger it with a configuration.

<img src="docs/images/trigger-pipeline-1.png" width="800">

In the configuration JSON, fill in the date argument in the `%Y-%m-%d %H:%M:%S.%f` format, as shown below.

Lastly, press the `Trigger` button.
<img src="docs/images/trigger-pipeline-2.png" width="800">
