import os.path
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed

import atlassian.errors
import pandas as pd
import matplotlib.colors as mc
import matplotlib.pyplot as plt
from matplotlib.offsetbox import AnchoredOffsetbox, TextArea, HPacker
import numpy as np
from src.common import ConfluenceConnection, ConfluenceNodeMapper


class DataManager:
    """
    This class Downloads attachments from Confluence and filters them for the needed attachments, because confluence
    api does not support single downloading attachments. The correct attachment will be stored in a file system and
    the remaining attachments will be deleted.
    """

    def __init__(self, confluence: ConfluenceConnection):
        self.__confluence = confluence
        self.generic_attachment_save_path = f"{os.getenv('DIR.RESOURCES')}/download"  # A temporary working file
        self.correct_attachment_save_path = f"{os.getenv('DIR.RESOURCES')}/stats"  # Path, where the correct file from temp dir will be moved to
        if not os.path.exists(self.correct_attachment_save_path):
            os.makedirs(self.correct_attachment_save_path)

    def __del__(self):
        self.__delete_stats_dir()

    def get_stat_file_from_page(self, page_id, filename: str):
        """
        This method downloads all available files from the page with the ID from confluence. The needed stats file will be moved
        to a permanent directory and the temporary directory will be deleted.
        """
        if not os.path.exists(self.generic_attachment_save_path):
            os.makedirs(self.generic_attachment_save_path)
        try:
            self.__confluence.download_attachments_from_page(page_id, path=self.generic_attachment_save_path)
            src = os.path.join(self.generic_attachment_save_path, filename)
            dest = os.path.join(self.correct_attachment_save_path, filename)
            file = self.__move_file(src, dest)
            self.__delete_temp_dir()
            return file
        except atlassian.errors.ApiError:
            print(page_id)
            return None

    def __move_file(self, src: str, dest: str):
        """
        This method moves files from a source to a destination path. When any kind of exception occurs, it will be
        catched, so the whole program can continue to run.
        """
        try:
            shutil.move(src, dest)
            return dest
        except Exception:
            print(f"Exception: Could not move file from {src} to {dest}")
            return None

    def __delete_temp_dir(self):
        dir = self.generic_attachment_save_path
        if os.path.exists(dir) and os.path.isdir(dir):
            shutil.rmtree(dir)
        else:
            print("Directory does not exist")

    def __delete_stats_dir(self):
        dir = self.correct_attachment_save_path
        if os.path.exists(dir) and os.path.isdir(dir):
            shutil.rmtree(dir)
        else:
            print("Directory does not exist")


class HeatMapFactory:
    def plot(self, data: dict, _dates: []):
        data = self.order_dict(data)
        clinics = []
        data_matrix = []
        for clinic in data:
            clinics.append(clinic)
            data_matrix.append(data[clinic])
        data_matrix = np.array(data_matrix)

        # Define the colors and thresholds (absolute values)
        colors = [
            'black',  # For values in the range < 0
            'darkblue',  # Prussian Blue for values [0.001, 0.049]
            'yellow',  # For values [0.05, 0.75]
            'red',  # For values [0.75, 1]
            'darkred'  # For values = 1
        ]
        bounds = [-1, -0.01, 5, 15, 30, 90]

        # Create the heatmap with its configurations
        cmap = mc.ListedColormap(colors)
        norm = mc.BoundaryNorm(bounds, cmap.N)
        plt.figure(figsize=(data_matrix.shape[1]/4, data_matrix.shape[0]/4))
        extent = (0, len(data_matrix[0]), 0, len(data_matrix))
        plt.imshow(data_matrix, cmap=cmap, norm=norm, aspect="auto", extent=extent)
        plt.colorbar(label="Error Rate in %")
        plt.subplots_adjust(left=0.2)

        # Create horizontal lines and clinic labels for y axis
        ticks = np.arange(len(data_matrix))
        plt.hlines(ticks, xmin=0, xmax=data_matrix.shape[1], color='grey', linewidth=0.5)
        label_ticks = ticks + .5
        plt.yticks(ticks=label_ticks, labels=clinics[::-1], fontsize=8)
        plt.xticks(ticks=np.arange(len(_dates)) + .25, labels=_dates,
           rotation=90, ha="left", fontsize=8)
        plt.savefig('heatmap.png')

    def order_dict(self, data: dict):
        sorted_data = dict(
            sorted(data.items(), key=lambda item: sum(item[1]), reverse=True)
        )
        return sorted_data


class ChartManager:

    def __init__(self, mapper: ConfluenceNodeMapper, csv_paths: [], save_path: str = "error_rates_histogram.png", max_days: int = 42):
        self.mapper = mapper
        self.csv_paths = csv_paths
        self.save_path = save_path
        self.max_days = max_days

    def heat_map(self):
        """
        This method manages the collection auf needed error rate data and initializes the Heatmap generation factory
        """
        hm = HeatMapFactory()
        _skipped_paths = []
        _data = {}

        def process_path(path):
            try:
                _dates, _error_rates = Helper.read_error_rates(path)
                _error_rates = _error_rates[-self.max_days:]
                _dates = _dates[-self.max_days:]

                clinic_id = Helper.get_clinic_num(path)
                clinic_name = self.mapper.get_node_value_from_mapping_dict(clinic_id, "COMMON_NAME")
                _data[clinic_name] = _error_rates
                return _data, _dates
            except Exception as e:
                print(f"Error processing {path}: {e}")
                return None

        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(process_path, path): path for path in self.csv_paths}
            for future in as_completed(futures):
                result = future.result()
                if result:
                    _data, _dates = result
        hm.plot(_data, _dates)
        plt.savefig(self.save_path)


class Helper:
    @staticmethod
    def get_clinic_num(path: str):
        """
        Returns a clinic number contained in a given path. Required syntax: .../{clinic num}_...
        """
        num = path.split('/')[-1].split("_")[0]
        return num

    @staticmethod
    def read_error_rates(csv_file):
        """
        This method extracts error rates and date information from their respective columns in a csv file. Empty error
        rates will be marked with a negative value
        """
        _error_rates_df = []

        _df = pd.read_csv(csv_file, sep=';')
        try:
            _df['date'] = pd.to_datetime(_df['date'], format='%Y-%m-%d %H:%M:%S.%f%z')
        except Exception as e:
            print(f'fixing error: {e}')
            _df = pd.read_csv(csv_file, sep=',')
            _df['date'] = pd.to_datetime(_df['date'], format='%Y-%m-%d %H:%M:%S.%f%z')
        _df = _df.sort_values(by='date')
        _date = [x.strftime('%d-%m') for x in _df['date']]

        _df[_df == '-'] = -10.00
        _df['daily_error_rate'] = _df['daily_error_rate'].apply(lambda x: float(x))
        _error_rates = _df['daily_error_rate'].to_numpy()

        return _date, _error_rates

