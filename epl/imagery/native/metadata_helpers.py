import copy
import re

from datetime import date, datetime, time
from enum import IntEnum
from typing import TypeVar
from peewee import Model, Field, FloatField, CharField, DateTimeField, IntegerField, Database, ModelSelect, CompoundSelectQuery

sql_reg = re.compile(r'SELECT[\s\S]+FROM[\s\S]+(AS[\s\S]+)\Z')


class _QueryParam:
    def __init__(self, field: Field):
        self.field = field
        self.values = []
        self.not_values = []

    def _set_value(self, value, b_equals):
        if value is date:
            value = datetime.combine(value, datetime.min.time())
        if value is datetime:
            value = value.isoformat()

        if b_equals:
            self.values.append(value)
        else:
            self.not_values.append(value)

    def set_value(self, value):
        self._set_value(value, True)

    def set_not_value(self, not_value):
        self._set_value(not_value, False)

    def append_select(self, p_select: ModelSelect):
        if not self.values and not self.not_values:
            return p_select

        if self.values:
            p_select = p_select.where(self.field << self.values)

        if self.not_values:
            p_select = p_select.where(~(self.field << self.not_values))

        return p_select


class _RangeQueryParam(_QueryParam):
    """for now can't handle multiple ranges. set range will clear out a previously designated range"""
    def __init__(self, field: Field):
        super().__init__(field)
        self.start = None
        self.end = None
        self.start_inclusive = True
        self.end_inclusive = True

    def set_range(self, start=None, start_inclusive=True, end=None, end_inclusive=True):
        if not start and not end:
            raise ValueError
        self.start = start
        self.end = end
        self.start_inclusive = start_inclusive
        self.end_inclusive = end_inclusive

    def append_select(self, p_select: ModelSelect):
        p_select = super().append_select(p_select)
        if self.start:
            if self.start_inclusive:
                p_select = p_select.where(self.field >= self.start)
            else:
                p_select = p_select.where(self.field > self.start)
        if self.end:
            if self.end_inclusive:
                p_select = p_select.where(self.field <= self.end)
            else:
                p_select = p_select.where(self.field < self.end)

        return p_select


class _DateQueryParam(_RangeQueryParam):
    D = TypeVar('D', date, datetime)

    @staticmethod
    def _get_date_string(value: D, time_part: time=None):
        if not value:
            return None
        if type(value) is date and time_part:
            value = datetime.combine(value, time_part)
        elif type(value) is not datetime:
            raise ValueError

        return '{}'.format(value.isoformat())

    def set_range(self, start: D=None, start_inclusive=True, end: D=None, end_inclusive=True):
        super().set_range(_DateQueryParam._get_date_string(start, datetime.min.time()), start_inclusive, _DateQueryParam._get_date_string(end, datetime.max.time()), end_inclusive)

    def set_value(self, value: D):
        if type(value) is date:
            self.set_range(datetime.combine(value, datetime.min.time()), True, datetime.combine(value, datetime.max.time()), True)
        elif type(value) is datetime:
            # TODO is this string conversion a bigquery landsat thing?
            super().set_value(_DateQueryParam._get_date_string(value))
        else:
            raise ValueError

    def set_not_value(self, not_value: D):
        if type(not_value) is date:
            self.set_range(datetime.combine(not_value, datetime.max.time()), False, datetime.combine(not_value, datetime.min.time()), False)
        elif type(not_value) is datetime:
            super().set_not_value(_DateQueryParam._get_date_string(not_value))
        else:
            raise ValueError


class _BoundQueryParam():
    """        north_lat = _RangeQueryParam("north_lat")
        south_lat = _RangeQueryParam("south_lat")
        west_lon = _RangeQueryParam("west_lon")
        east_lon = _RangeQueryParam("east_lon")"""
    def __init__(self, north_field: Field, south_field: Field, west_field: Field, east_field: Field):
        self.north_field = north_field
        self.south_field = south_field
        self.west_field = west_field
        self.east_field = east_field
        self.bounds = []

    def set_bounds(self, west: float=None, south: float=None, east: float=None, north: float=None):
        if not west:
            west = -180
        if not east:
            east = 180
        if not south:
            south = -90
        if not north:
            north = 90

        if east > west:
            self.bounds.append((west, south, east, north))
        else:
            # TODO split the bounds into to sets on either side of the dateline
            raise ValueError

    def append_select(self, p_select: ModelSelect):
        if not self.bounds:
            return p_select

        expression = None
        for bounds in self.bounds:
            minx = bounds[0]
            miny = bounds[1]
            maxx = bounds[2]
            maxy = bounds[3]

            a = (minx <= self.west_field).bin_and((maxx >= self.west_field))
            b = (minx >= self.west_field).bin_and((self.east_field >= minx))
            ab = a.bin_or(b)
            c = (self.south_field <= miny).bin_and((self.north_field >= miny))
            d = (self.south_field > miny).bin_and((maxy >= self.south_field))
            cd = c.bin_or(d)
            abcd = ab.bin_and(cd)
            expression_part = abcd

            if not expression:
                expression = expression_part
            else:
                expression.bin_or(expression_part)

        return p_select.where(expression)


class MetadataModel(Model):
    cloud_cover = FloatField()
    acquired = DateTimeField()
    north_lat = FloatField()
    south_lat = FloatField()
    west_lon = FloatField()
    east_lon = FloatField()

    class Meta:
        database = Database("[bigquery-public-data:cloud_storage_geo_index.landsat_index]")


class LandsatModel(MetadataModel):
    scene_id = CharField()
    product_id = CharField()
    spacecraft_id = CharField()
    sensor_id = CharField()
    collection_number = CharField()
    collection_category = CharField()
    data_type = CharField()
    base_url = CharField()

    sensing_time = DateTimeField()

    wrs_path = IntegerField()
    wrs_row = IntegerField()

    total_size = IntegerField()


class MetadataFilters:
    def __init__(self):
        self.model = MetadataModel
        self.cloud_cover = _RangeQueryParam(MetadataModel.cloud_cover)
        self.acquired = _DateQueryParam(MetadataModel.acquired)
        self.bounds = _BoundQueryParam(MetadataModel.north_lat, MetadataModel.south_lat, MetadataModel.west_lon, MetadataModel.east_lon)
        self.spacecraft_id = _QueryParam(LandsatModel.spacecraft_id)
        # self.geometry_wkb = None

    @staticmethod
    def param_sequence(params):
        for i, param in enumerate(params):
            if isinstance(param, str):
                param = '"{}"'.format(param)
            params[i] = param
        return params

    def get_select(self):
        sorted_keys = sorted(self.__dict__)
        model_select = self.model.select()
        for key in sorted_keys:
            if key == "model":
                continue

            model_select = self.__dict__[key].append_select(model_select)

        return model_select

    def get_sql(self, limit=10, sort_by_field: Field=None):

        select_statement = self.get_select()
        if sort_by_field:
            select_statement = select_statement.order_by(sort_by_field)
        else:
            select_statement = select_statement.order_by(self.acquired.field.desc())

        sql, params = select_statement.sql()

        sql = re.sub('\"t1\"\.\"([\w]+)\"', r't1.\1', sql)
        sql = sql.replace('"t1"', 't1')
        # fix params so that strings have quote symbol
        params = MetadataFilters.param_sequence(params)
        sql = sql.replace('?', '{}')
        sql = sql.format(*params)

        regex_results = sql_reg.search(sql)
        sql_formatted = 'SELECT * FROM [bigquery-public-data:cloud_storage_geo_index.landsat_index] {}'.format(regex_results.group(1))
        # sql_formatted = sql_formatted.replace('"t1".', '')
        return "{} LIMIT {}".format(sql_formatted, limit)


class LandsatQueryFilters(MetadataFilters):

    def __init__(self):
        super().__init__()
        self.model = LandsatModel

        self.cloud_cover = _RangeQueryParam(LandsatModel.cloud_cover)
        self.acquired = _DateQueryParam(LandsatModel.acquired)

        self.bounds = _BoundQueryParam(LandsatModel.north_lat, LandsatModel.south_lat, LandsatModel.west_lon,
                                       LandsatModel.east_lon)

        self.acquired = _DateQueryParam(LandsatModel.sensing_time)
        self.spacecraft_id = _QueryParam(LandsatModel.spacecraft_id)

        self.scene_id = _QueryParam(LandsatModel.scene_id)
        self.product_id = _QueryParam(LandsatModel.product_id)
        self.sensor_id = _QueryParam(LandsatModel.sensor_id)
        self.collection_number = _QueryParam(LandsatModel.collection_number)
        self.collection_category = _QueryParam(LandsatModel.collection_category)
        self.data_type = _QueryParam(LandsatModel.data_type)
        self.base_url = _QueryParam(LandsatModel.base_url)

        self.wrs_path = _RangeQueryParam(LandsatModel.wrs_path)
        self.wrs_row = _RangeQueryParam(LandsatModel.wrs_row)

        # self.polygon_wkbs = []
        # self.envelopes = []

        self.total_size = _RangeQueryParam(LandsatModel.total_size)





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