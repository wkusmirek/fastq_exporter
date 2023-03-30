import argparse
import json
from minknow_api.manager import Manager
from collections import Counter

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
    
    channel_count = connection.device.get_flow_cell_info().channel_count
    channel_states = connection.data.get_channel_states(
        wait_for_processing=True,
        first_channel=1,
        last_channel=channel_count,
    )
    
    channel_states_dict = {i: None for i in range(1, channel_count + 1)}
    for state in channel_states:
        for channel in state.channel_states:
            channel_states_dict[int(channel.channel)] = channel.state_name

    counted = Counter(channel_states_dict.values())

    result = list()
    vars = dict()
    vars['address'] = str(args.host) + ':' + str(args.port)
    vars['name'] = 'asd'
    for it in counted.most_common():
        vars[it[0]] = str(it[1])
    result.append(vars)

    print('[' + ','.join(json.dumps(d) for d in result) + ']')

if __name__ == "__main__":
    main()
