# tetralone-ABA-simulation
Details and code for the simulation of tetralone-ABA's impact on grapevine cold hardiness in North America.<br>

## Training of the mutated NYUS.2.1 model to simulate the impact of tetralone-ABA on grapevine cold hardiness (under /model_training)
* The 'Autogluon_model_training.ipynb' file contains the code for model generation and model distilling.
* The above notebook will generate a model (folder) named 'mutated_NYUS_2_1', which is a ready-to-use model for the tetralone-ABA impact simulation.
* The 'All_training_data_tetralone_ABA_NYUS_2_1.csv' contains all the cold hardiness data for NYUS.2.1 training along with the cold hardiness measurement data (control and tetralone-ABA data from 'Riesling', 'Aravelle' and 'Cayuga White') in the two tetralone-ABA experiments conducted between 2022-2024.<br>

## North America cold hardiness prediction using the mutated NYUS.2.1 model (under /model_prediction_NA_data)
* The 'Weather_data_processing.R' contains the code for North America weather data download from [GHCNd](https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily) and feature extraction.
* The .py files are ready-to-use scripts for model prediction for different cultivars with or without tetralone-ABA treatment. There should be a 'mutated_NYUS_2_1' model folder under the same dir to execute these scripts.
* The resutling .csv files will contain all the prediction cold hardiness data, which matches the order of the input data.
