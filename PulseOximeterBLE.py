# Módulo PulseOximeterBLE: Conexión, lectura de datos y almacenamiento.
# Author: Ignacio Hernández

import _bleio
import adafruit_ble

from adafruit_ble import BLERadio
from adafruit_ble.advertising.standard import Advertisement
from adafruit_ble.services.standard.device_info import DeviceInfoService
from adafruit_ble_berrymed_pulse_oximeter import BerryMedPulseOximeterService

import pandas as pd
import time

class PulseOximeterBLE:
    """
    Controlador del pulsioxímetro Berry para la toma de datos:
    - Pulso o BPMs (beats per minute)
    - Saturación de oxígeno o SpO2
    """

    connection_error = ConnectionError
    if hasattr(_bleio, 'ConnectionError'):
        connection_error = _bleio.ConnectionError

    def __init__(self, verbose=True):
        self.verbose = verbose
        self.connection = None

    @property
    def connected(self):
        if self.connection:
            return self.connection.connected
        else:
            return False

    # --- ESTABLECER LA CONEXIÓN --- #
    def connect_pulse_oximeter(self, target="BerryMed", timeout=15):
        """
        1- Buscar dispositivos Bluetooth disponibles.
        2- Establecer conexión con el cual tenga de nombre 'target'
        """

        self.ble_radio = adafruit_ble.BLERadio()
        self.connection = None

        found_devices = set()

        print(f"Buscando dispositivos Bluetooth...\n- Objetivo: '{target}'")
        for advert in self.ble_radio.start_scan(Advertisement, timeout=timeout):
            name = advert.complete_name
            if not name: continue

            name = name.strip('\x00') # Posibles nulls en el nombre

            # Dispositivo encontrado
            if name == target:
                print(f"\nEstableciendo conexión con '{name}'...")
                self.connection = self.ble_radio.connect(advert)
                break

            elif name not in found_devices and self.verbose:
                print(f"Encontrado '{name}'.")
                found_devices.add(name)

        # Detener búsqueda
        self.ble_radio.stop_scan()

        if self.connection and self.connection.connected:
            print("=> Dispositivo conectado")
        else:
            print(f"No se ha encontrado '{target}'. Escaneo detenido.")
    
    def disconnect_pulse_oximeter(self):
        """Desconectar pulsioxímetro. Hacerse en caso de fallo de conexión."""
        try:
            self.connection.disconnect()
        except connection_error:
            pass

        self.connection = None

    # --- LECTURA DE DATOS --- #
    # Una vez conectado al dispositivo, recabar info sobre su fabricante y modelo
    def read_device_info(self):
        """Leer datos sobre el aparato"""
        if DeviceInfoService in self.connection:
            device = self.connection[DeviceInfoService]
            
            # Manufacturer
            try:
                self.manufacturer = device.manufacturer
            except AttributeError:
                self.manufacturer = "(Manufacturer Not specified)"
            
            # Model Number
            try:
                self.model_number = device.model_number
            except AttributeError:
                self.model_number = "(Model number not specified)"
                
            print("Device:", self.manufacturer, self.model_number, '\n')
            
        else:
            print("Sin información del dispositivo.\n")

    def receive_data(self):
        """Recoger los datos tomados por el pulsioximetro"""
        service = self.connection[BerryMedPulseOximeterService]

        # Series temporales
        pulse_list = list()
        spo2_list  = list()
        full_record= list()

        print("--- Lectura comenzada ---\n")
        
        # Marcadores temporales
        timestamps = list()
        t0 = time.perf_counter()

        # Lectura
        while self.connection.connected:
            read_data = service.values

            if read_data:
                [valid, SpO2, BPM, pleth, finger_in] = read_data

                valid_sample = valid and finger_in and BPM < 255

                if valid_sample:
                    t = time.perf_counter() - t0
                    t = round(t, 3)
                    timestamps.append(t)

                    if self.verbose: print(f"Pulso: {BPM}, SpO2: {SpO2}")

                    # Records
                    pulse_list.append(BPM)
                    spo2_list.append(SpO2)
                    full_record.append(read_data)

        print("\n--- Lectura finalizada ---")

        # Almacenar datos obtenidos
        self.BPM_series  = pd.Series(pulse_list, index=timestamps)
        self.SpO2_series = pd.Series(spo2_list,  index=timestamps)
        self.to_dataframe()

        print("=> Dispositivo desconectado")

    def to_dataframe(self):
        """Recoger los datos obtenidos en pd.Series a un DataFrame"""
        df = pd.DataFrame()
        df['BPM']  = self.BPM_series
        df['SpO2'] = self.SpO2_series

        self.dataframe = df
        return df

    def read(self):
        """
        1- Lectura de datos del dispositivo
        2- Toma de datos del pulsioximetro
        """
        if self.connection and self.connection.connected:
            print("Obteniendo datos del dispositivo...")
            
            # 1- Información del dispositivo
            self.read_device_info()

            # 2- Extracción de datos continua
            try:
                self.receive_data()
            except connection_error:
                connection = disconnect_pulse_oximeter()