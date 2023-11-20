#!/usr/bin/env python
import os
import kaggle

dataset_name = 'vikasukani/loan-eligible-dataset'



kaggle.api.dataset_download_files(dataset_name, path='.', unzip=True)
