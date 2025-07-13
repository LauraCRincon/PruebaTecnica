import re
import pandas as pd
from Loader import Loader
from pandas import DataFrame
from datetime import datetime


class Factory:
    """This object extract, transforma and load data.

    Attributes:
        loader: Object has resources for extract and load the data.
        sp: Name of store procedure for process data.

    Methods:
        selectPipeline(): Select the pipeline for specific path.
        pipeline(): Transform the data and uses functions of Loader.
    """

    def __init__(self) -> None:
        self.loader = Loader()
        self.sp = ""
        pass

    def selectPipeline(self, path: str) -> None:
        """Select the pipeline for specific path.

        Args:
            path: Path the BLOB that triggered the cloud function.

        Returns:
            Nothing.
        """

        if 'transacciones' in path:
            self.pipeline(path)
        else:
            print("No hay Pipeline para este archivo")

        return

    def pipeline(self, path):
        """Transform the data and uses functions of Loader.

        Args:
            path: Path the BLOB that triggered the cloud function.

        Returns:
            Una descripción del valor de retorno.
        """
        file_name = path
        load_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        df = pd.read_csv(file_name, dtype= str)

        news_headers = ['TipoIdentificacion', 'NumeroIdentificacion', 'NumeroCuenta', 'Nombres', 
                        'TipoTransaccion', 'MontoTransaccion', 'TipoProducto', 'Ciudad', 'FechaHora', 
                        'FechaNacimiento', 'DireccionCliente', 'TelefonoCliente', 'CorreoCliente', 
                        'ReporteCentralesRiesgo', 'MontoReporteCentralRiesgo', 'TiempoMoraReporteRiesgo'
                        ]
        df.columns = news_headers

        float_cols = ['MontoTransaccion', 'MontoReporteCentralRiesgo']
        date_cols =['FechaNacimiento']

        data_types = {
            'float': float_cols,
            'datetime64[ns]': date_cols
            }
        
        for dtype, cols in data_types.items():
            for col in cols:
                df[col] = df[col].astype(dtype)   

        # 1. Intentamos convertir todo a datetime. Los valores tipo '2024-02-28 08:48:17' se convertirán bien, los Unix quedarán NaT
        df['Fecha_Hora_temp'] = pd.to_datetime(df['FechaHora'], errors='coerce')

        # 2. Para los valores que no se pudieron convertir, asumimos que son timestamps Unix
        mask_failed_conversion = df['Fecha_Hora_temp'].isna()

        # Verifica que los Unix están realmente en número (pueden venir como string)
        df.loc[mask_failed_conversion, 'Fecha_Hora_temp'] = pd.to_datetime(
            df.loc[mask_failed_conversion, 'FechaHora'].astype(float),  # <-- usa float por si vienen como string
            unit='s',
            errors='coerce'
        )

        # 3. Asignamos la columna buena a FechaHora
        df['FechaHora'] = df['Fecha_Hora_temp']
        df.drop(columns=['Fecha_Hora_temp'], inplace=True)

        #Validaciones

        df = df.dropna(subset=['NumeroIdentificacion', 'FechaHora', 'MontoTransaccion'])
        df = df[df['FechaHora'].notna()]
        df = df[df['MontoTransaccion'] >= 0]

        df.insert(loc=len(df.columns), column='Archivo', value=file_name)
        df.insert(loc=len(df.columns), column='FechaCarga', value=load_date)

        print(f"Cargando {len(df)} filas a BigQuery...")  # 👈 Aquí lo agregas

        self.loader.uploadBigQuery(
            df=df,
            dataset='raw_transacciones',
            table='transacciones',
            action='replace',
            clustering_fields=['NumeroIdentificacion', 'TipoProducto', 'TipoTransaccion'])

#        self.loader.callProcedure(
#            sp='BancoBogotaPrueba.processed_transacciones.sp_clean_transacciones')
#        
#        self.loader.callProcedure(
#            sp='bancobogotaprueba.processed_transacciones.sp_insert_transactions')

        print(f"Se cargo correctamente la data {file_name}")
        return
