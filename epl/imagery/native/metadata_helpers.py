import copy

from datetime import date
from datetime import datetime
from enum import IntEnum


class _QueryParam:
    def __init__(self, param_name: str):
        self.param_name = param_name
        self.value = None
        self.equals = True

    def set_value(self, value):
        if value is date:
            value = datetime.combine(value, datetime.min.time())
        if value is datetime:
            value = value.isoformat()

        self.value = value
        self.equals = True

    def set_not_value(self, not_value):
        self.value = not_value
        self.equals = False

    def get(self, sql_message="", b_start=False):
        if self.value is None:
            return sql_message

        sql_message += " WHERE " if b_start or len(sql_message) == 0 else " AND "
        if self.value is not None:
            operand = "=" if self.equals else "!="
            if isinstance(self.value, str):
                sql_message += '{0}{1}"{2}"'.format(self.param_name, operand, self.value)
            else:
                sql_message += '{0}{1}{2}'.format(self.param_name, operand, self.value)

        return sql_message


class _RangeQueryParam(_QueryParam):
    def __init__(self, param_name: str):
        super().__init__(param_name)
        self.start = None
        self.end = None
        self.start_inclusive = False
        self.end_inclusive = False

    def set_range_start(self, start, start_inclusive=True):
        self.value = None
        self.start = start
        self.start_inclusive = start_inclusive

    def set_range_end(self, end, end_inclusive=False):
        self.value = None
        self.end = end
        self.end_inclusive = end_inclusive

    def set_value(self, value):
        super().set_value(value)
        self.end = None
        self.start = None

    def set_not_value(self, not_value):
        super().set_not_value(not_value)
        self.end = None
        self.start = None

    def _get_range(self, sql_message=""):
        if self.start is not None:
            operand = ">=" if self.start_inclusive else ">"
            sql_message += "{0}{1}{2}".format(self.param_name, operand, self.start)
            if self.end is not None:
                sql_message = "{0} AND ".format(sql_message)
        if self.end is not None:
            operand = "<=" if self.end_inclusive else "<"
            sql_message += "{0}{1}{2}".format(self.param_name, operand, self.end)

        return sql_message

    def get(self, sql_message="", b_start=False):
        if self.value is None and self.start is None and self.end is None:
            return sql_message

        if self.value is not None:
            sql_message = super().get(sql_message)
        else:
            sql_message += " WHERE " if b_start or len(sql_message) == 0 else " AND "
            sql_message = self._get_range(sql_message)

        return sql_message


class _DateQueryParam(_RangeQueryParam):

    @staticmethod
    def _get_date_string(value: date or datetime):
        if type(value) is date:
            value = datetime.combine(value, datetime.min.time())
        elif type(value) is not datetime:
            raise ValueError

        return '"{}"'.format(value.isoformat())

    def set_range_start(self, start: date or datetime, start_inclusive=True):
        super().set_range_start(start, start_inclusive)
        self.start = _DateQueryParam._get_date_string(start)

    def set_range_end(self, end: date or datetime, end_inclusive=False):
        super().set_range_end(end, end_inclusive)
        self.end = _DateQueryParam._get_date_string(end)

    def set_value(self, value: date or datetime):
        if type(value) is date:
            self.set_range_start(datetime.combine(value, datetime.min.time()), True)
            self.set_range_end(datetime.combine(value, datetime.max.time()), True)
        elif type(value) is not datetime:
            raise ValueError
        else:
            super().set_value(value)
            self.value = _DateQueryParam._get_date_string(value)

    def set_not_value(self, not_value: date or datetime):
        if type(not_value) is date:
            self.set_range_start(datetime.combine(not_value, datetime.max.time()), False)
            self.set_range_end(datetime.combine(not_value, datetime.min.time()), False)
        elif type(not_value) is not datetime:
            raise ValueError
        else:
            super().set_not_value(not_value)
            self.value = _DateQueryParam._get_date_string(not_value)


class MetadataFilters:
    def __init__(self):
        self.cloud_cover = _RangeQueryParam("cloud_cover")
        self.acquired = None
        # self.geometry_wkb = None

    def get(self, sql_message="", b_start=False):
        sorted_keys = sorted(self.__dict__)
        sql_len = len(sql_message)
        for key in sorted_keys:
            sql_message = self.__dict__[key].get(sql_message, b_start)
            if sql_len < len(sql_message):
                b_start = False

        return sql_message


class LandsatQueryFilters(MetadataFilters):
    def __init__(self):
        super().__init__()
        self.scene_id = _QueryParam("scene_id")
        self.product_id = _QueryParam("product_id")
        self.spacecraft_id = _QueryParam("spacecraft_id")
        self.sensor_id = _QueryParam("sensor_id")
        self.collection_number = _QueryParam("collection_number")
        self.collection_category = _QueryParam("collection_category")
        self.data_type = _QueryParam("data_type")
        self.base_url = _QueryParam("base_url")

        self.acquired = _DateQueryParam("sensing_time")

        self.wrs_path = _RangeQueryParam("wrs_path")
        self.wrs_row = _RangeQueryParam("wrs_row")

        # north_lat = _RangeQueryParam("north_lat")
        # south_lat = _RangeQueryParam("south_lat")
        # west_lon = _RangeQueryParam("west_lon")
        # east_lon = _RangeQueryParam("east_lon")
        self.total_size = _RangeQueryParam("total_size")




    """
        scene_id	STRING	REQUIRED 	Unique identifier for a particular Landsat image downlinked to a particular ground station.
        product_id	STRING	NULLABLE	Unique identifier for a particular scene processed by the USGS at a particular time, or null for pre-collection data.
        spacecraft_id	STRING	NULLABLE	The spacecraft that acquired this scene: one of 'LANDSAT_4' through 'LANDSAT_8'.
        sensor_id	STRING	NULLABLE	The type of spacecraft sensor that acquired this scene: 'TM' for the Thematic Mapper, 'ETM' for the Enhanced Thematic Mapper+, or 'OLI/TIRS' for the Operational Land Imager and Thermal Infrared Sensor.
        collection_number	STRING	NULLABLE	The Landsat collection that this image belongs to, e.g. '01' for Collection 1 or 'PRE' for pre-collection data.
        collection_category	STRING	NULLABLE	Indicates the processing level of the image: 'RT' for real-time, 'T1' for Tier 1, 'T2' for Tier 2, and 'N/A' for pre-collection data. RT images will be replaced with Tier 1 or Tier 2 images as they become available.
        data_type	STRING	NULLABLE	The type of processed image, e.g. 'L1T' for Level 1 terrain-corrected images.
        base_url	STRING	NULLABLE	The base URL for this scene in Cloud Storage.

        date_acquired	STRING	NULLABLE	The date on which this scene was acquired (UTC).
        sensing_time	STRING	NULLABLE	The approximate time at which this scene was acquired (UTC).

        wrs_path	INTEGER	NULLABLE	The path number of this scene's location in the Worldwide Reference System (WRS).
        wrs_row	INTEGER	NULLABLE	The row number of this scene's location in the Worldwide Reference System (WRS).
        cloud_cover	FLOAT	NULLABLE	Estimated percentage of this scene affected by cloud cover.
        north_lat	FLOAT	NULLABLE	The northern latitude of the bounding box of this scene.
        south_lat	FLOAT	NULLABLE	The southern latitude of the bounding box of this scene.
        west_lon	FLOAT	NULLABLE	The western longitude of the bounding box of this scene.
        east_lon	FLOAT	NULLABLE	The eastern longitude of the bounding box of this scene.
        total_size	INTEGER	NULLABLE	The total size of this scene in bytes.
    """


class DataDeviceSubClass(IntEnum):
    pass


class DataDeviceClass(IntEnum):
    pass


# TODO this should be IntFlag to allow for combinations
class SpacecraftID(DataDeviceSubClass):
    UNKNOWN_SPACECRAFT = 0
    LANDSAT_1_MSS = 1
    LANDSAT_2_MSS = 2
    LANDSAT_3_MSS = 4
    LANDSAT_123_MSS = 7
    LANDSAT_4_MSS = 8
    LANDSAT_5_MSS = 16
    LANDSAT_45_MSS = 24
    LANDSAT_4 = 32
    LANDSAT_5 = 64
    LANDSAT_45 = 96
    LANDSAT_7 = 128
    LANDSAT_8 = 256
    LANDSAT = 512


class Band(IntEnum):
    # Crazy Values so that the Band.<ENUM>.value isn't used for anything
    UNKNOWN_BAND = 0
    ULTRA_BLUE = 1001
    BLUE = 1002
    GREEN = 1003
    RED = 1004
    NIR = 1005
    SWIR1 = 1006
    THERMAL = 1007
    SWIR2 = 1008
    PANCHROMATIC = 1009
    CIRRUS = 1010
    TIRS1 = 1011
    TIRS2 = 1012
    INFRARED2 = 1013
    INFRARED1 = 1014
    ALPHA = 1015


class BandMap:
    # TODO it would be nice to store data type, Byte, Unit16, etc.
    __map = {
        # TODO min resolution??
        SpacecraftID.LANDSAT_8: {
            # TODO min resolution should be 15 for LANDSAT_8?
            'max_resolution': 30,
            Band.ULTRA_BLUE: {
                'number': 1,
                'wavelength_range': (0.435, 0.451),
                'description': 'Coastal and aerosol studies',
                'resolution_m': 30},
            Band.BLUE: {'number': 2, 'wavelength_range': (0.452, 0.512),
                        'description': 'Bathymetric mapping, distinguishing soil from vegetation, and deciduous from coniferous vegetation',
                        'resolution_m': 30},
            Band.GREEN: {'number': 3, 'wavelength_range': (0.533, 0.590),
                         'description': 'Emphasizes peak vegetation, which is useful for assessing plant vigor',
                         'resolution_m': 30},
            Band.RED: {'number': 4, 'wavelength_range': (0.636, 0.673),
                       'description': 'Discriminates vegetation slopes', 'resolution_m': 30},
            Band.NIR: {'number': 5, 'wavelength_range': (0.851, 0.879),
                       'description': 'Emphasizes biomass content and shorelines', 'resolution_m': 30},
            Band.SWIR1: {'number': 6, 'wavelength_range': (1.566, 1.651),
                         'description': 'Discriminates moisture content of soil and vegetation; penetrates thin clouds',
                         'resolution_m': 30},
            Band.SWIR2: {'number': 7, 'wavelength_range': (2.107, 2.294),
                         'description': 'Improved moisture content of soil and vegetation and thin cloud penetration',
                         'resolution_m': 30},
            Band.PANCHROMATIC: {'number': 8, 'wavelength_range': (0.503, 0.676),
                                'description': '15 meter resolution, sharper image definition', 'resolution_m': 15},
            Band.CIRRUS: {'number': 9, 'wavelength_range': (1.363, 1.384),
                          'description': 'Improved detection of cirrus cloud contamination', 'resolution_m': 30},
            Band.TIRS1: {'number': 10, 'wavelength_range': (10.60, 11.19),
                         'description': '100 meter resolution, thermal mapping and estimated soil moisture',
                         'resolution_m': 30},
            Band.TIRS2: {'number': 11, 'wavelength_range': (11.50, 12.51),
                         'description': '100 meter resolution, Improved thermal mapping and estimated soil moisture',
                         'resolution_m': 30},
        },
        SpacecraftID.LANDSAT_45: {
            'max_resolution': 30,
            Band.BLUE: {'number': 1, 'wavelength_range': (0.45, 0.52),
                        'description': 'Bathymetric mapping, distinguishing soil from vegetation, and deciduous from coniferous vegetation',
                        'resolution_m': 30},
            Band.GREEN: {'number': 2, 'wavelength_range': (0.52, 0.60),
                         'description': 'Emphasizes peak vegetation, which is useful for assessing plant vigor',
                         'resolution_m': 30},
            Band.RED: {'number': 3, 'wavelength_range': (0.63, 0.69), 'description': 'Discriminates vegetation slopes',
                       'resolution_m': 30},
            Band.NIR: {'number': 4, 'wavelength_range': (0.77, 0.90),
                       'description': 'Emphasizes biomass content and shorelines', 'resolution_m': 30},
            Band.SWIR1: {'number': 5, 'wavelength_range': (1.55, 1.75),
                         'description': 'Discriminates moisture content of soil and vegetation; penetrates thin clouds',
                         'resolution_m': 30},
            Band.THERMAL: {'number': 6, 'wavelength_range': (10.40, 12.50),
                           'description': 'Thermal mapping and estimated soil moisture (60m downsample Landsat7, 120m downsample landsat 4&5)',
                           'resolution_m': 30},
            Band.SWIR2: {'number': 7, 'wavelength_range': (2.09, 2.35),
                         'description': 'Hydrothermally altered rocks associated with mineral deposits',
                         'resolution_m': 30},
        },
        SpacecraftID.LANDSAT_123_MSS: {
            'max_resolution': 60,
            Band.GREEN: {'number': 4, 'wavelength_range': (0.5, 0.6),
                         'description': 'Sediment-laden water, delineates areas of shallow water', 'resolution_m': 60},
            Band.RED: {'number': 5, 'wavelength_range': (0.6, 0.7), 'description': 'Cultural features',
                       'resolution_m': 60},
            Band.INFRARED1: {'number': 6, 'wavelength_range': (0.7, 0.8),
                             'description': 'Vegetation boundary between land and water, and landforms',
                             'resolution_m': 60},
            Band.INFRARED2: {'number': 7, 'wavelength_range': (0.8, 1.1),
                             'description': 'Penetrates atmospheric haze best, emphasizes vegetation, boundary between land and water, and landforms',
                             'resolution_m': 60},
        },
        SpacecraftID.LANDSAT_45_MSS: {
            'max_resolution': 60,
            Band.GREEN: {'number': 1, 'wavelength_range': (0.5, 0.6),
                         'description': 'Sediment-laden water, delineates areas of shallow water', 'resolution_m': 60},
            Band.RED: {'number': 2, 'wavelength_range': (0.6, 0.7), 'description': 'Cultural features',
                       'resolution_m': 60},
            Band.INFRARED1: {'number': 3, 'wavelength_range': (0.7, 0.8),
                             'description': 'Vegetation boundary between land and water, and landforms',
                             'resolution_m': 60},
            Band.INFRARED2: {'number': 4, 'wavelength_range': (0.8, 1.1),
                             'description': 'Penetrates atmospheric haze best, emphasizes vegetation, boundary between land and water, and landforms',
                             'resolution_m': 60},
        }
    }

    # shallow copy
    __map[SpacecraftID.LANDSAT_7] = copy.copy(__map[SpacecraftID.LANDSAT_45])
    __map[SpacecraftID.LANDSAT_7][Band.PANCHROMATIC] = {'number': 8, 'wavelength_range': (0.52, 0.90),
                                                        'description': '15 meter resolution, sharper image definition',
                                                        'resolution_m': 15}

    __enum_map = {}
    for spacecrafID in __map:
        for band_key in __map[spacecrafID]:
            if isinstance(__map[spacecrafID][band_key], dict):
                if spacecrafID not in __enum_map:
                    __enum_map[spacecrafID] = {}
                __enum_map[spacecrafID][__map[spacecrafID][band_key]['number']] = band_key

    def __init__(self, spacecraft_id: SpacecraftID):
        if spacecraft_id & SpacecraftID.LANDSAT_123_MSS:
            self.__spacecraft_id = SpacecraftID.LANDSAT_123_MSS
        elif spacecraft_id & SpacecraftID.LANDSAT_45_MSS:
            self.__spacecraft_id = SpacecraftID.LANDSAT_45_MSS
        elif spacecraft_id & SpacecraftID.LANDSAT_45:
            self.__spacecraft_id = SpacecraftID.LANDSAT_45
        elif spacecraft_id & SpacecraftID.LANDSAT_7:
            self.__spacecraft_id = SpacecraftID.LANDSAT_7
        elif spacecraft_id == SpacecraftID.LANDSAT_8:
            self.__spacecraft_id = SpacecraftID.LANDSAT_8
        else:
            self.__spacecraft_id = None

    def get_name(self, band_number):
        return self.__enum_map[self.__spacecraft_id][band_number].name

    def get_band_enum(self, band_number):
        return self.__enum_map[self.__spacecraft_id][band_number]

    def get_number(self, band_enum: Band):
        return self.__map[self.__spacecraft_id][band_enum]['number']

    def get_resolution(self, band_enum: Band):
        return self.__map[self.__spacecraft_id][band_enum]['resolution_m']

    def get_details(self):
        return self.__map[self.__spacecraft_id]

    def get_max_resolution(self):
        return self.__map[self.__spacecraft_id]['max_resolution']