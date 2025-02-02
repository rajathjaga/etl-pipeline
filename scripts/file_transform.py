import pandas as pd
import logging
import xml.etree.ElementTree as ET
import os

def ensure_directory_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def transform_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        if 'height_in' in df.columns:
            df['height_m'] = df['height_in'] * 0.0254
        if 'weight_lb' in df.columns:
            df['weight_kg'] = df['weight_lb'] * 0.453592
        transformed_path = os.path.join(
            "data", "transformed", os.path.basename(file_path).replace('.csv', '_transformed.csv')
            )
        ensure_directory_exists("data/transformed")
        df.to_csv(transformed_path, index=False)
        logging.info(f"CSV file transformed and saved to {transformed_path}.")
        return transformed_path
    except Exception as e:
        logging.error(f"Error during CSV transformation: {e}")
        return None

def transform_json(file_path):
    try:
        df = pd.read_json(file_path, lines=True)
        # Perform transformations (example: convert height from inches to meters)
        df['height_m'] = df['height'] * 0.0254
        df['weight_kg'] = df['weight'] * 0.453592

        # Save the transformed DataFrame to a new file
        transformed_path = os.path.join(
            "data", "transformed", os.path.basename(file_path).replace('.json', '_transformed.csv')
        )
        df.to_csv(transformed_path, index=False)
        return transformed_path

    except Exception as e:
        print(f"Error during transformation: {e}")
        return None

def transform_xml(file_path):
    try:
        # Parse the XML file
        tree = ET.parse(file_path)
        root = tree.getroot()

        # Extract data
        data = []
        for person in root.findall("person"):
            name = person.find("name").text
            height = float(person.find("height").text)
            weight = float(person.find("weight").text)
            data.append({"name": name, "height": height, "weight": weight})

        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Perform transformations (example: convert height from inches to meters)
        df['height_m'] = df['height'] * 0.0254
        df['weight_kg'] = df['weight'] * 0.453592

        # Save the transformed DataFrame to a new file
        transformed_path = os.path.join(
            "data", "transformed", os.path.basename(file_path).replace('.xml', '_transformed.csv')
        )
        df.to_csv(transformed_path, index=False)
        print(f"Transformed data saved to {transformed_path}")
        return transformed_path

    except Exception as e:
        print(f"Error during transformation: {e}")
        return None
