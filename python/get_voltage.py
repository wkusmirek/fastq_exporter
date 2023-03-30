import argparse
import json
from minknow_api.manager import Manager

def main():
    parser = argparse.ArgumentParser(
        description="List sequencing positions connected to a host."
    )
    parser.add_argument(
        "--host", default="localhost", help="Specify which host to connect to."
    )
    parser.add_argument(
        "--port", default=9502, help="Specify which port to connect to."
    )

    args = parser.parse_args()

    manager = Manager(host=args.host, port=args.port)

    positions = list(manager.flow_cell_positions())
    filtered_positions=positions

    connection = filtered_positions[0].connect()
    
    voltage_data = connection.device.get_bias_voltage()

    result = list()
    vars = dict()
    vars['address'] = str(args.host) + ':' + str(args.port)
    vars['name'] = 'asd'
    vars['bias_voltage'] = str(voltage_data.bias_voltage)
    result.append(vars)

    print('[' + ','.join(json.dumps(d) for d in result) + ']')

if __name__ == "__main__":
    main()
