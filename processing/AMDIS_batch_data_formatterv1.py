import os
import pandas as pd
import xml.etree.ElementTree as ET
import io
import numpy as np
from pathlib import Path
from psims.mzml.writer import MzMLWriter
from psims.mzml.components import InstrumentConfiguration, ComponentList, Source, Analyzer, Detector


def batch_process_mzml(input_root: str, output_root: str, intensity_multiplier: float = 1e16, decimal_places: int = 3, progress_signal = None, message_signal = None, pstart = 0, total_files = 0):
    """
    Recursively processes all .tst files in directory tree

    Parameters:
        input_root (str): Root directory containing .tst files
        output_root (str): Output directory for mzML files
        intensity_multiplier (float): Scaling factor for intensity values
        decimal_places (int): Decimal places for rounding
    """
    processed_files = pstart
    errors = []

    # Recursive directory traversal with os.walk
    for root, dirs, files in os.walk(input_root):
        for filename in files:
            if filename.lower().endswith('.msv'):
                input_path = os.path.join(root, filename)

                try:
                    # Create output path structure
                    relative_path = Path(root).relative_to(input_root)
                    output_dir = Path(output_root) / relative_path
                    output_dir.mkdir(parents=True, exist_ok=True)

                    output_filename = f"{Path(filename).stem}.mzML"
                    output_path = output_dir / output_filename

                    # Process individual file
                    process_single_file(
                        input_path=input_path,
                        output_path=output_path,
                        intensity_multiplier=intensity_multiplier,
                        decimal_places=decimal_places
                    )
                    processed_files += 1
                    progress = int((processed_files / total_files) * 100)
                    progress_signal.emit(progress)
                    message_signal.emit(f"Processed: {input_path} → {output_path}")

                except Exception as e:
                    errors.append(f"{input_path} - {str(e)}")

    # Print summary
    print(f"\nBatch processing complete")
    print(f"Errors encountered: {len(errors)}")
    if errors:
        print("\nError details:")
        for error in errors:
            print(f"• {error}")
    return processed_files


def process_single_file(input_path: str,
                        output_path: str,
                        intensity_multiplier: float,
                        decimal_places: int):
    """Process individual TST file to mzML format"""
    # XML data extraction
    tree = ET.parse(input_path)
    root = tree.getroot()

    # Data processing
    for MSDATA in root.iter('DATA'):
        raw_data = MSDATA.text

    df_raw = pd.read_csv(io.StringIO(raw_data), sep=';')
    df_processed = process_dataframe(df_raw, intensity_multiplier, decimal_places)
    df_long = pd.melt(df_processed, id_vars="Retention Time",
                      var_name="m/z", value_name="intensity")

    # mzML writing
    with open(output_path, 'wb') as outfile, MzMLWriter(outfile) as writer:
        write_mzml_metadata(writer)
        with writer.run(id="run1", instrument_configuration="instrument1"):
            write_spectra(writer, df_long.groupby('Retention Time'))
            write_chromatogram(writer, df_processed)

    # Process the written file to move scan time
    process_mzml_file(output_path, os.path.dirname(output_path))


def process_dataframe(df: pd.DataFrame,
                      multiplier: float,
                      decimals: int) -> pd.DataFrame:
    """Process and clean raw dataframe"""
    df = df.drop(['RT(minutes) - NOT USED BY IMPORT', 'RI'], axis=1)
    df = df.rename(columns={'RT(milliseconds)': 'Retention Time'})

    # Process numerical data
    subset = df.iloc[:, 1:].mask(df.iloc[:, 1:] < 0, 0.000)
    df.iloc[:, 1:] = subset.mul(multiplier).round(decimals)
    #Time is converted to seconds
    df['Retention Time'] = df['Retention Time'].astype(float) / 1000

    return df


def write_mzml_metadata(writer: MzMLWriter):
    """Write common mzML metadata"""
    writer.controlled_vocabularies()
    writer.file_description(file_contents=["MS1 Spectrum"])
    writer.software_list([{"id": "psims_converter", "version": "1.0"}])

    # Instrument configuration
    components = ComponentList([
        Source(1, ["electrospray ionization"]),
        Analyzer(2, ["time-of-flight"]),
        Detector(3, ["electron multiplier"])
    ])
    instrument = InstrumentConfiguration("instrument1", components, ["MS:1000483"])
    writer.register("InstrumentConfiguration", "instrument1")
    writer.instrument_configuration_list([instrument])

    # Data processing info
    processing = writer.ProcessingMethod(1, "psims_converter",
                                         ["Conversion to mzML", "centroid spectrum"])
    writer.data_processing_list([writer.DataProcessing([processing], 'DP1')])


def write_spectra(writer: MzMLWriter, grouped_data):
    """Write spectrum data to mzML"""
    with writer.spectrum_list(count=len(grouped_data)):
        for i, (rt, group) in enumerate(grouped_data):
            writer.write_spectrum(
                group['m/z'].values,
                group['intensity'].values,
                id=f"scan={i + 1}",
                params=[
                    "MS1 Spectrum",
                    {"ms level": 1},
                    {"total ion current": np.sum(group['intensity'].values)},
                    {"scan start time": rt, "unitName": "second"}
                ]
            )


def write_chromatogram(writer: MzMLWriter, df: pd.DataFrame):
    """Write chromatogram data to mzML"""
    rt_array = df['Retention Time'].values.astype(float)
    int_array = df.iloc[:, 1:].sum(axis=1).values.astype(float)

    with writer.chromatogram_list(1):
        writer.write_chromatogram(
            rt_array,
            int_array,
            id="TIC",
            chromatogram_type="total ion current chromatogram",
            params=[
                {"time array": {"unitName": "second"}},
                {"intensity array": {"unitName": "counts"}}
            ]
        )
# mzML default namespace of the files so ET can match it
NS_URI = "http://psi.hupo.org/ms/mzml"
XSI_URI = "http://www.w3.org/2001/XMLSchema-instance"

# ET requires a defined prefix mapping for XPath
NS = {"mzml": NS_URI}

def process_mzml_file(filepath, output_dir):
    tree = ET.parse(filepath)
    root = tree.getroot()  # <indexedmzML>

    # Get inner <mzML> element
    mzml_elem = root.find("mzml:mzML", NS)
    if mzml_elem is None:
        print(f"Skipped: <mzML> not found in {filepath}")
        return

    # Find all <spectrum> elements using the namespace
    spectra = mzml_elem.findall(".//mzml:spectrum", NS)


    #Loop over <spectrum> elements
    for spectrum in spectra:
        # Get all <cvParam> children of this <spectrum>
        cv_params = spectrum.findall("mzml:cvParam", NS)

        # Find all "scan start time" cvParams
        scan_start_params = [cv for cv in cv_params if cv.attrib.get("accession") == "MS:1000016"]

        #Loop to find <scan> in spectrum element data
        for scan_start in scan_start_params:
            # Remove the original from spectrum if needed
            #spectrum.remove(scan_start)

            # Find <scan> inside <scanList>
            scan_elem = spectrum.find(".//mzml:scan", NS)

            # If <scan> exists, append the line
            if scan_elem is not None:
                scan_elem.append(scan_start)

    # Save to output directory
    base_name = os.path.basename(filepath)
    out_path = os.path.join(output_dir, f"{base_name}")

    #Have to do this next part because ET strips some namespaces and they need to be forced back in

    # Register default and xsi namespaces
    ET.register_namespace('', NS_URI)
    ET.register_namespace('xsi', XSI_URI)

    # Set schemaLocation on both indexedmzML and mzML
    root.set(f"{{{XSI_URI}}}schemaLocation","http://psi.hupo.org/ms/mzml http://psidev.info/files/ms/mzML/xsd/mzML1.1.3_idx.xsd")

    mzml_elem.set("xmlns", NS_URI)  # default namespace
    mzml_elem.set("xmlns:xsi", XSI_URI)  # explicitly sets xmlns:xsi
    mzml_elem.set(f"{{{XSI_URI}}}schemaLocation",
                  "http://psi.hupo.org/ms/mzml http://psidev.info/files/ms/mzML/xsd/mzML1.1.1.xsd")

    #write file
    tree.write(out_path, encoding="utf-8", xml_declaration=True)
    print(f"Processed: {base_name}")
