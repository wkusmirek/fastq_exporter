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
    
    current_acquisition_run = connection.acquisition.get_current_acquisition_run()

    base_calling_enabled = False
    if current_acquisition_run:
      base_calling_enabled = (current_acquisition_run.config_summary.basecalling_enabled)

    rltype = 2 if base_calling_enabled else 1
    histogram = connection.statistics.stream_read_length_histogram(
                            read_length_type=rltype,
                            bucket_value_type=1,
                            acquisition_run_id=current_acquisition_run.run_id,
                        )
    for h in histogram:
      #self.histogram_data = histogram_event
      print(h)
      #if acquisition_status not in {"ACQUISITION_RUNNING","ACQUISITION_STARTING",}:
      #  break

    result = list()
    vars = dict()
    #vars['read_count'] = str(current_acquisition_run_data.yield_summary.read_count)
    result.append(vars)

    print('[' + ','.join(json.dumps(d) for d in result) + ']')

if __name__ == "__main__":
    main()
