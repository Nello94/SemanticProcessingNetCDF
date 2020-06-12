from netCDF4 import Dataset
import os
import datetime


class NetCDF2JSON(object):

    filename = None

    def __init__(self, filename):
        self.filename = filename

    def as_json(self):

        try:
            rootgrp = Dataset(self.filename)
        except FileNotFoundError:
            return None

        lon1 = None
        lon2 = None
        lat1 = None
        lat2 = None
        geo = False

        if geo is False:
            try:
                lon1 = float(min(rootgrp.variables['lon']))
                lon2 = float(max(rootgrp.variables['lon']))
                lat1 = float(min(rootgrp.variables['lat']))
                lat2 = float(max(rootgrp.variables['lat']))
                geo = True
            except KeyError:
                pass

        if geo is False:
            try:
                lon1 = float(min(rootgrp.variables['X']))
                lon2 = float(max(rootgrp.variables['X']))
                lat1 = float(min(rootgrp.variables['Y']))
                lat2 = float(max(rootgrp.variables['Y']))
                geo = True
            except KeyError:
                pass

        if geo is False:
            try:
                lon1 = float(min(rootgrp.variables['longitude']))
                lon2 = float(max(rootgrp.variables['longitude']))
                lat1 = float(min(rootgrp.variables['latitude']))
                lat2 = float(max(rootgrp.variables['latitude']))
                geo = True
            except KeyError:
                pass

        variables = []
        for variable in rootgrp.variables.values():
            attributes = []
            for attribute in variable.ncattrs():
                attributes.append({"name": str(attribute), "value": str(variable.getncattr(attribute))})

            dimensions = []
            for dimension in variable.dimensions:
                dimensions.append(str(dimension))

            shapes = []
            for shape in variable.shape:
                shapes.append(shape)

            variables.append({
                "name": str(variable.name),
                "dtype": str(variable.dtype),
                "ndim": variable.ndim,
                "shape": shapes,
                "dimensions": dimensions,
                "attributes": attributes
            })

        dimensions = []
        for dimension in rootgrp.dimensions.values():
            dimensions.append({
                "name": str(dimension.name),
                "size": len(dimension),
            })

        attributes = []
        for attribute in rootgrp.ncattrs():
            attributes.append({"name": str(attribute), "value": str(rootgrp.getncattr(attribute))})

        feature = {
            "name": os.path.basename(self.filename),
            "dimensions": dimensions,
            "variables": variables,
            "date": str(datetime.datetime.utcnow()),
            "attributes": attributes
        }
        if geo is True:
            feature["loc"] = {
                "type": "Polygon",
                "coordinates": [[[lon1, lat1], [lon2, lat1], [lon2, lat2], [lon1, lat2], [lon1, lat1]]]
            }
        rootgrp.close()

        return feature
