import argparse
import json
from minknow_api.manager import Manager


def main():
    """Main entrypoint for list_sequencing_devices example"""
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

    # Construct a manager using the host + port provided.
    manager = Manager(host=args.host, port=args.port)

    # Find a list of currently available sequencing positions.
    positions = manager.flow_cell_positions()

    result = list()
    for pos in positions:
        vars = dict()
        vars['address'] = str(args.host) + ':' + str(args.port)
        vars['name'] = pos.name
        vars['state'] = pos.state
        vars['port'] = pos.description.rpc_ports.secure
        result.append(vars)
    print('[' + ','.join(json.dumps(d) for d in result) + ']')

if __name__ == "__main__":
    main()
