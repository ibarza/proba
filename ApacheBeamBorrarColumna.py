import argparse
from datetime import datetime
import pandas as pd
import apache_beam as beam
from apache_beam import Map, Flatten
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.io.textio import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions


def csv_to_dict(line):
    """ Transformar CSV a Diccionario """
    Columnas = []
    Besteak = []
    if Columnas == []:
        for i in line.split(","):
            if i == line.split(",")[0]:
                Columnas == []
            else:
                Columnas.append(i)
    else:
        balioak = line.split(",")
        dic = {}
        for i in range(len(balioak)-1):
            dic[Columnas[i]] = balioak[i+1]
        Besteak.append(dic)


    # TODO: realizar las transformaciones necesarias
    # line = '0,2022-09-19,9899,90000757,0.0,-0.15,0.0,0.0,0.0,0.0,-0.15,0.0,0.0,0.0,0.0,0.0'

    return line


if __name__ == '__main__':
    # TODO: especificar la dirección del fichero
    file_path = r'C:\Users\mispracticas.vpo\Desktop\test_ventas.csv'
    df = pd.read_csv('test_ventas.csv')




    parser = argparse.ArgumentParser()

    # Argumentos necesarios para ejecutar el Pipeline.
    # parser.add_argument('--runner', required=True)
    # parser.add_argument('--project', required=True)
    # parser.add_argument('--region', required=True)
    # parser.add_argument('--staging_location', required=True)
    # parser.add_argument('--temp_location', required=True)
    # parser.add_argument('--network', required=True)
    # parser.add_argument('--subnetwork', required=True)
    # parser.add_argument('--job_name', required=True)
    # parser.add_argument('--input', required=True)
    # parser.add_argument('--output', required=True)

    known_args, pipeline_args = parser.parse_known_args()

    # Especificamos la configuración del pipeline
    beam_options = PipelineOptions(
        # runner=known_args.runner,
        # project=known_args.project,
        # temp_location=known_args.temp_location,
        # staging_location=known_args.staging_location,
        # save_main_session=True,
        # region=known_args.region,
        # network=known_args.network,
        # subnetwork=known_args.subnetwork,
        # job_name=known_args.job_name,
    )

    with beam.Pipeline(options=beam_options) as p:
        (p | 'Read FRES' >> ReadFromText(file_path, skip_header_lines=1)
         | 'Make Dic FRES' >> Map(csv_to_dict)
         | 'aldatu izena' >> WriteToText(file_path_prefix=r'C:\Users\mispracticas.vpo\Desktop\test_ventas_zutabeaezabatu.csv')
         # | 'Write BigQuery' >> WriteToBigQuery(
         #            table=known_args.output,
         #            create_disposition=BigQueryDisposition.CREATE_NEVER,
         #            write_disposition=BigQueryDisposition.WRITE_TRUNCATE
         #        )
         )