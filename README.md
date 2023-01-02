# Airflow-computervision

This is the airflow repository of the computer vision team.
This repository contains the pipelines that are used in our container project.

We created the pipelines in this repository based on the following image in Miro.

As you can see in the image below our pipeline is split in three main parts;
We will refer to each of the three main parts also as *pipelines*.

1. This pipeline is responsible for retrieving images from cloud, storing them in our storage account from Azure 
and running 2 AI models, the blurring model and the container detection model. 
It also contains tasks to remove the images from the storage account once they're processed. 
This first pipeline is going to be run multiple times in order to optimise the large amount of data that must be processed.


2. This pipeline contains only one task end is responsible for combining the predictions of the container model with 
two other sources namely the permit data from Decos and the locations of vulnerable bridges. 
This pipeline also creates the maps in HTML format which summarise the current situation of where illegal containers 
are present in the city centre.


3. This pipeline is responsible for sending a signal to see you based on the content of the maps produced by the previous pipeline. 

Each pipeline is triggered manually from airflow. When we trigger one pipeline we trigger it with a configuration 
which contains the date argument in the format written below.