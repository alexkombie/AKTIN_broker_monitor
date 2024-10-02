import os.path
import random
import shutil
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
        self.generic_attachment_save_path = f"{os.getenv('DIR.RESOURCES')}/tmp"  # A temporary working file
        self.correct_attachment_save_path = f"{os.getenv('DIR.RESOURCES')}/stats"  # Path, where the correct file from temp dir will be moved to
        if not os.path.exists(self.correct_attachment_save_path):
            os.makedirs(self.correct_attachment_save_path)

    def __del__(self):
        self.__delete_stats_dir()

    def get_stat_file_from_page(self, page_id, filename: str):
        """
        This method downloads all available files of the page ID from confluence. The needed stats file will be moved
        to a permanent directory and the temporary directory will be deleted.
        """
        if not os.path.exists(self.generic_attachment_save_path):
            os.makedirs(self.generic_attachment_save_path)
        self.__confluence.download_attachments_from_page(page_id, path=self.generic_attachment_save_path)
        src = os.path.join(self.generic_attachment_save_path, filename)
        dest = os.path.join(self.correct_attachment_save_path, filename)
        file = self.__move_file(src, dest)
        self.__delete_temp_dir()
        return file

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


class LineChartFactory:
    def __init__(self, threashold: float, observation_time: int):
        self.x_labels = None
        self._DAYS = None
        self._threashold = threashold
        self._observation_time = observation_time
        self._unmarked_chart_color = "gainsboro"
        self._y_top_lim = 30.0
        self._clinic_label_positions = []  # represents a grid, in wich graph labels will be placed to avoid overlapping
        self._label_font_size = 12
        plt.figure(figsize=(20, 10))
        plt.axhline(y=self._threashold, color='lightgray', linestyle='--', linewidth=1)

    """The first plot needs to span from the first to the last day without missing values"""  # Todo fix

    def plot(self, _dates: np.array, _error_rates: np.array, clinic_name: str):
        if self.x_labels is None:
            self.x_labels = _dates
        _clinic_day_reports = self._DAYS
        if _clinic_day_reports > len(_error_rates):
            _clinic_day_reports = len(_error_rates)
            _dates = _dates[-(_clinic_day_reports + 1):]
            _error_rate = _error_rates[-(_clinic_day_reports + 1):]
        else:
            _dates = _dates[-(_clinic_day_reports + 1):-1]
            _error_rate = _error_rates[-(_clinic_day_reports + 1):-1]
        _error_rate = np.array(list(map(float, _error_rate)))
        color = self.get_color_for_graph(_error_rate, self._threashold, self._observation_time)
        if color.__eq__(self._unmarked_chart_color):
            plt.plot(_dates, _error_rate, marker=',', linestyle='-', linewidth=1, color=color, label='', zorder=1)
        else:
            plt.plot(_dates, _error_rate, marker=',', linestyle='-', linewidth=1, color=color, label=clinic_name,
                     zorder=2)
            self._clinic_label_positions.append({"y": _error_rate[-1], "name": clinic_name, "col": color})

    def place_clinic_name_labels(self, _dates):
        # Create clinic name at the end of an highlighted graph
        if _dates is not None and len(_dates) > 0 and len(self._clinic_label_positions) > 0:
            _label_matrix = sorted(self._clinic_label_positions, key=lambda x: x["y"])
            _position_matrix = [[]]  # This variable groups clinic names that would otherwise overlap each other

            for i in range(len(_label_matrix)):
                _label = _label_matrix[i]
                if i == 0:
                    _position_matrix[-1].append(_label)
                else:
                    _prev = _label_matrix[i - 1]
                    if _label["y"] - _prev["y"] <= 1.0:
                        _position_matrix[-1].append(_label)
                    else:
                        _position_matrix.append([_label])

            for tuples in _position_matrix:
                _y = tuples[0]["y"]
                self._pack_labels_together(_y, tuples)

    def _pack_labels_together(self, _y: float, tuples: []):
        _text_areas = []
        for t in tuples:
            _text_areas.append(TextArea(t["name"], textprops=dict(color=t["col"], fontsize=self._label_font_size)))

        # Combine them into a single label using VPacker
        combined_text = HPacker(children=_text_areas, align="center", pad=0, sep=0)

        # Create an anchored offset box to place the combined text on the plot
        anchored_box = AnchoredOffsetbox(loc='center', child=combined_text, pad=0, frameon=False,
                                         bbox_to_anchor=(self._DAYS - 0.5, _y),  # Adjust (1.05, _y) to position
                                         bbox_transform=plt.gca().transData, borderpad=0.)

        plt.gca().add_artist(anchored_box)

    def get_color_for_graph(self, values, threashold, observation_time):
        recent_values = values[-observation_time:]
        is_over_threashold = any(value > threashold for value in recent_values)
        if is_over_threashold:
            return Helper.random_color()
        else:
            return self._unmarked_chart_color

    def save(self, filename):
        self.place_clinic_name_labels(self.x_labels)
        plt.xlim(right=self._DAYS)
        plt.ylim(bottom=-10, top=30)

        plt.xticks(rotation=45, fontsize=10)
        y_ticks = plt.yticks()[0]
        # Create custom labels
        y_labels = [f"N/A" if tick == -10 else str(tick) for tick in y_ticks]
        plt.yticks(y_ticks, y_labels, fontsize=10)

        plt.ylabel('Fehlerrate [%]')
        plt.legend()

        plt.savefig(filename)
        plt.clf()

    def set_days(self, days: int):
        self._DAYS = days


class ChartManager:

    def __init__(self, mapper: ConfluenceNodeMapper, csv_paths: [], save_path: str = "error_rates_histogram.png", max_days: int = 42):
        self.mapper = mapper
        self.csv_paths = csv_paths
        self.save_path = save_path
        self.max_days = max_days

    def mult_line_chart(self):
        lc = LineChartFactory(threashold=5.0, observation_time=7)
        _is_first = True  # True if no graph has been plotted yet
        _skipped_paths = []
        for path in self.csv_paths:
            try:
                _dates, _error_rates = Helper.read_error_rates(path)
            except Exception as e:
                print(e)
                continue

            if _is_first and len(_dates) < self.max_days:
                self.max_days = len(_dates)
            lc.set_days(self.max_days)
            clinic_id = Helper.get_clinic_num(path)
            clinic_name = self.mapper.get_node_value_from_mapping_dict(clinic_id, "COMMON_NAME")
            lc.plot(_dates, _error_rates, clinic_name)
            _is_first = False

        lc.save(self.save_path)


class Helper:
    @staticmethod
    def get_clinic_num(path: str):
        """
        Returns a clinic number contained in a given path. Required syntax: .../{clinic num}_...
        """
        num = path.split('/')[-1].split("_")[0]
        return num

    @staticmethod
    def random_color():
        """
        Generates a random color in hexadecimal format.

        Returns:
        - A string representing a random color.
        """
        color = ''
        keys = list(mc.cnames.keys())
        while color.__eq__('') or color.__contains__('white') or color.__contains__('gray') or Helper.brightness(
                mc.to_rgb(color)) > 0.8:
            color = random.choice(list(keys))
        return color

    @staticmethod
    def brightness(rgb):
        return 0.299 * rgb[0] + 0.587 * rgb[1] + 0.114 * rgb[2]

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
        _error_rates = _df['daily_error_rate'].to_numpy()
        _error_rates[_error_rates == '-'] = '-10.00'

        return _date, _error_rates

