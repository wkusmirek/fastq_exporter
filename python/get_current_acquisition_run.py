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
    
    current_acquisition_run_data = connection.acquisition.get_current_acquisition_run()

    result = list()
    vars = dict()
    vars['address'] = str(args.host) + ':' + str(args.port)
    vars['name'] = 'asd'
    vars['seconds'] = '60'
    vars['read_count'] = str(current_acquisition_run_data.yield_summary.read_count)
    vars['fraction_basecalled'] = str(current_acquisition_run_data.yield_summary.fraction_basecalled)
    vars['fraction_skipped'] = str(current_acquisition_run_data.yield_summary.fraction_skipped)
    vars['basecalled_pass_read_count'] = str(current_acquisition_run_data.yield_summary.basecalled_pass_read_count)
    vars['basecalled_fail_read_count'] = str(current_acquisition_run_data.yield_summary.basecalled_fail_read_count)
    vars['basecalled_skipped_read_count'] = str(current_acquisition_run_data.yield_summary.basecalled_skipped_read_count)
    vars['basecalled_pass_bases'] = str(current_acquisition_run_data.yield_summary.basecalled_pass_bases)
    vars['basecalled_fail_bases'] = str(current_acquisition_run_data.yield_summary.basecalled_fail_bases)
    vars['basecalled_samples'] = str(current_acquisition_run_data.yield_summary.basecalled_samples)
    vars['selected_raw_samples'] = str(current_acquisition_run_data.yield_summary.selected_raw_samples)
    vars['selected_events'] = str(current_acquisition_run_data.yield_summary.selected_events)
    vars['estimated_selected_bases'] = str(current_acquisition_run_data.yield_summary.estimated_selected_bases)
    vars['alignment_matches'] = str(current_acquisition_run_data.yield_summary.alignment_matches)
    vars['alignment_mismatches'] = str(current_acquisition_run_data.yield_summary.alignment_mismatches)
    vars['alignment_insertions'] = str(current_acquisition_run_data.yield_summary.alignment_insertions)
    vars['alignment_deletions'] = str(current_acquisition_run_data.yield_summary.alignment_deletions)
    vars['alignment_coverage'] = str(current_acquisition_run_data.yield_summary.alignment_coverage)
    result.append(vars)

    print('[' + ','.join(json.dumps(d) for d in result) + ']')

if __name__ == "__main__":
    main()
