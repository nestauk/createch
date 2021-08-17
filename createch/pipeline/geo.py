# Scripts to woth with geo data

import json
import logging
import os
import requests
from io import BytesIO
from zipfile import ZipFile

import geopandas as gp
import altair as alt

from createch import PROJECT_DIR

SHAPE_PATH = f"{PROJECT_DIR}/inputs/data/ttwa_shapefile"

# def fetch_shape(shape_path):
#     """Fetch shapefile data"""

#     shape19_url =

#     response = requests.get(shape19_url)

#     my_zip = ZipFile(BytesIO(response.content))

#     my_zip.extractall(shape_path)


def read_shape():

    shapef = (
        gp.read_file(
            f"{SHAPE_PATH}/Travel_to_Work_Areas__December_2011__Super_Generalised_Clipped_Boundaries_in_United_Kingdom.shp"
        )
        .to_crs(epsg=4326)
        .assign(id=lambda x: x.index.astype(int))
    )

    return shapef


def plot_choro(
    shapef,
    count_var,
    count_var_name,
    region_name="region",
    scheme="spectral",
    scale_type="linear",
):
    """This function plots an altair choropleth

    Args:
        shapef (json) is the the json version of a geopandas shapefile.
        count_var (str) is the name of the variable we are plotting
        count_var_name (str) tidy name for the count variable
        scale_type (str) is the type of scale we are using. Defaults to log
        region_name (str) is the name of the region variable
        schemes (str) is the colour scheme. Defaults to spectral
    """

    base_map = (  # Base chart with outlines
        alt.Chart(alt.Data(values=shapef["features"]))
        .project(type="mercator")
        .mark_geoshape(filled=False, stroke="gray")
    )

    choropleth = (  # Filled polygons and tooltip
        base_map.transform_calculate(region=f"datum.properties.{region_name}")
        .mark_geoshape(filled=True, stroke="darkgrey", strokeWidth=0.2)
        .encode(
            size=f"properties.{count_var}:Q",
            color=alt.Color(
                f"properties.{count_var}:Q",
                title="National share",
                scale=alt.Scale(scheme="Spectral", type=scale_type),
                sort="descending",
            ),
            tooltip=[
                "region:N",
                alt.Tooltip(f"properties.{count_var}:Q", format="1.2f"),
            ],
        )
    )

    comb = base_map + choropleth
    return comb.properties(title=count_var_name)


def plot_counts(
    sh,
    geo_activity,
    count_var,
    name,
    exposure_var="category",
    scale_type="linear",
):
    """Plots exposure choropleth
    Args:
        sh (geodf): shapefile
        exposure_df (df): exposure shares
        month (int): month to visualise
        exposure (int): threshold for high exposure
        name (str): title for exposure variable
        exposure_var (str): name for exposure variable
    """

    merged = sh.merge(
        geo_activity, left_on="ttwa11nm", right_on="ttwa_name", how="outer"
    ).fillna(value={"share": 0.00001})

    merged_json = json.loads(merged.to_json())

    my_map = plot_choro(merged_json, count_var, name, "ttwa11nm", scale_type=scale_type)
    return my_map
