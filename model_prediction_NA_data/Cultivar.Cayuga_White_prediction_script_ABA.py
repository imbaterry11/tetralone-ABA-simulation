import autogluon
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import gc
#import pathlib
from autogluon.tabular import TabularDataset, TabularPredictor
#temp = pathlib.PosixPath
#pathlib.PosixPath = pathlib.WindowsPath
df=pd.read_csv(r'data_for_autogluon_prediction.csv', sep=',', header = 0, index_col= 0, engine = 'pyarrow')
gc.collect()
df_trans = df.drop(['ID','season','Year','Month','Day','lat','lon','Median_temp'],axis = 1)
df_trans['Cultivar.Riesling'] = 0
df_trans['Cultivar.Cayuga_White'] = 1
df_trans['Treatment.tetralone.ABA'] = 1
test_data = df_trans

predictor_LTE = TabularPredictor.load('mutated_NYUS_2_1',require_version_match=False)

#LightGBM_DSTL is the top distilled model
y_pred = predictor_LTE.predict(test_data,model='LightGBM_DSTL')
y_pred.to_csv(r'y_pred_Cultivar.Cayuga_White_ABA.csv', index = False, header=True)

gc.collect()


    
