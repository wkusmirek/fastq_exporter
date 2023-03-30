# sudo kill $(ps aux | grep '9502' | awk '{print $2}')
from uuid import uuid4
import uuid
import logging
from pathlib import Path
from threading import Thread
import time
import grpc
import numpy as np
import sys, csv, time
import time, random
from minknow_server import ManagerServer, SequencingPositionServer, FlowCellInfo, PositionInfo
from minknow_api import acquisition_pb2, manager_pb2, protocol_pb2, statistics_pb2

LOGGER = logging.getLogger(__name__)

minknowDefaultPort=9502
port0=8000
port1=8001
port2=8002
port3=8003
port4=8004
name0='X1'
name1='X2'
name2='X3'
name3='X4'
name4='X5'

reader = csv.DictReader(open('tests/test_data/FAH55104_SARS-CoV2_Tests_live_records.csv'))
test_data = {}
for row in reader:
    for column, value in row.items():
        test_data.setdefault(column, []).append(value)

TEST_ACQUISITION = acquisition_pb2.AcquisitionRunInfo(run_id=str(uuid.uuid4()))

TEST_PROTOCOL = protocol_pb2.ProtocolRunInfo(
    run_id=str(uuid.uuid4()),
)
TEST_PROTOCOL_WITH_ACQUISTIIONS = protocol_pb2.ProtocolRunInfo(
    run_id=str(uuid.uuid4()), acquisition_run_ids=[TEST_ACQUISITION.run_id]
)

def generateTestAcquistionOutputStats():
    epoch_time = int(time.time())
    test_data_csv_index = int(epoch_time/30) % 979 # 979 rows in csv file (30 sec interval)
    read_count = int(test_data['minKNOW_read_count'][test_data_csv_index])
    print(read_count)
    TEST_ACQUISITION_OUTPUT_STATS = [
        statistics_pb2.StreamAcquisitionOutputResponse(
            snapshots=[
                statistics_pb2.StreamAcquisitionOutputResponse.FilteredSnapshots(
                    filtering=[
                        statistics_pb2.AcquisitionOutputKey(
                            barcode_name="barcode1234",
                            lamp_barcode_id="unclassified",
                            lamp_target_id="unclassified",
                            alignment_reference="unaligned",
                        )
                    ],
                    snapshots=[
                        statistics_pb2.AcquisitionOutputSnapshot(
                            seconds=60,
                            yield_summary=acquisition_pb2.AcquisitionYieldSummary(
                                # Number of reads selected by analysis as good reads.
                                #
                                # The reads in this counter are completed, but not necessarily on disk yet.
                                read_count = read_count,
                                # This is the fraction of whole reads that the base-caller has finished
                                # with. The value should be in the range [0.0, 1.0]
                                #
                                # When base-calling is enabled, it can be added to fraction_skipped and
                                # multiplied by 100 to give the percentage of reads processed and by
                                # implication, the percentage of reads the user is waiting for the
                                # base-caller to process.
                                #
                                # Since 5.0
                                fraction_basecalled = random.uniform(0.60,0.70),

                                # This is the fraction of whole reads that have been skipped. The value
                                # should be in the range [0.0, 1.0]
                                #
                                # Since 5.0
                                fraction_skipped = random.uniform(0.10,0.15),

                                # Number of reads successfully basecalled.
                                basecalled_pass_read_count = round(read_count*random.uniform(0.60,0.70)*0.9),

                                # Number of reads which have failed to basecall.
                                basecalled_fail_read_count = round(read_count*random.uniform(0.60,0.70)*0.1),

                                # Number of reads which have been skipped
                                basecalled_skipped_read_count = round(read_count*random.uniform(0.10,0.15)),

                                # Number of bases which have been called and classed as pass.
                                basecalled_pass_bases = round(read_count*random.uniform(0.60,0.70)*0.9)*10000,

                                # Number of bases which have been called and were classed as fail.
                                basecalled_fail_bases = round(read_count*random.uniform(0.60,0.70)*0.1)*10000,

                                # Number of raw samples which have been called.
                                basecalled_samples = 1,

                                # Number of minknow raw samples which have been selected
                                # for writing to disk as reads.
                                selected_raw_samples = 1,

                                # Number of minknow events which have been selected
                                # for writing to disk as reads.
                                selected_events = 1,

                                # Estimated number of bases MinKNOW has selected for writing.
                                # This is estimated based on already called bases and samples.
                                estimated_selected_bases = 9,

                                # Number of bases which have matched target reference.
                                #
                                # Only specified when running live alignment.
                                #
                                # Since 4.0
                                alignment_matches = 14,

                                # Number of bases which have not matched target reference.
                                #
                                # Only specified when running live alignment.
                                #
                                # Since 4.0
                                alignment_mismatches = 15,

                                # Number of bases which were inserted into
                                # alignments that matched the reference.
                                #
                                # Only specified when running live alignment.
                                #
                                # Since 4.0
                                alignment_insertions = 16,

                                # Number of bases which were deleted from
                                # alignments that matched the reference.
                                #
                                # Only specified when running live alignment.
                                #
                                # Since 4.0
                                alignment_deletions = 17,

                                # Number of bases that match the target reference(s) expressed as a
                                # fraction of the total size of the target reference(s).
                                #
                                # eg: For a specified alignment-targets with 2000 and 3000 bases, if
                                # "alignment_matches" is 2500, then "alignment_coverage" will be 0.5
                                #
                                # Since 4.3
                                alignment_coverage = 19
                            ),
                        )
                    ],
                )
            ]
        )
    ]
    return TEST_ACQUISITION_OUTPUT_STATS

def generateTestDutyTimeStats():
    sequencing = statistics_pb2.StreamDutyTimeResponse.ChannelStateData(state_times=[1,3])
    TEST_DUTY_TIME_STATS = [
        statistics_pb2.StreamDutyTimeResponse(
            bucket_ranges = [
                statistics_pb2.StreamDutyTimeResponse.BucketRange(start=1,end=2000),
                statistics_pb2.StreamDutyTimeResponse.BucketRange(start=2000,end=3000),
                statistics_pb2.StreamDutyTimeResponse.BucketRange(start=3000,end=4000),
            ],
            channel_states = {
                'sequencing': sequencing
            }
        )
    ]
    return TEST_DUTY_TIME_STATS

def mock():
    positions = [
        #STATE_INITIALISING = 0;
        #STATE_RUNNING = 1;
        #STATE_RESETTING = 2;
        #STATE_HARDWARE_REMOVED = 3;
        #STATE_HARDWARE_ERROR = 4;
        #STATE_SOFTWARE_ERROR = 5;
        #STATE_NEEDS_ASSOCIATION = 6;
#      name:
#          The name of the position.  For MinIONs, this is the name of
#          the MinION (eg: MN12345). For integrated positions, this is
#          the label for the position on the sequencer (eg: X1 for
#          GridION, 1-A1-D1 for PromethION).
#      location:
#          For integrated flow cell positions, indicates where it is on
#          the sequencing unit.  This information is not provided for
#          MinIONs (except for the MinION Mk1C, in which case the
#          position is always 0, 0).
#      state:
#          The state of the flow cell position.  If the state is not
#          `STATE_RUNNING` or `STATE_INITIALISING`, the flow cell
#          position can be assumed to be unusable, and the `error_info`
#          field should be populated.
#      rpc_ports:
#          The ports the APIs for this flow cell position are provided
#          on.  Always provided if `state` is `STATE_RUNNING`. May also
#          be provided when `state` is one of the hardware errors if the
#          software is still running.
#      error_info:
#          Provides a textual description of error states.  When `state`
#          is not `STATE_INITIALISING`, `STATE_RUNNING` or
#          `STATE_RESETTING`, this provides some information (in English)
#          about the error. This will be a textual description of the
#          value in `state`, possibly with extra information about the
#          error (if available).  This can be useful for dealing with
#          (new) unknown states.
#      shared_hardware_group:
#          Some positions may share hardware. Positions that share
#          hardware will have the same group-id. If positions do share
#          hardware, to reset the hardware you will need to reset all
#          positions in the group at the same time.
#      is_integrated:
#          Indicates that this is an integrated flow cell position.  This
#          is true for the integrated positions on a PromethION, GridION
#          or MinION Mk1C, and false for a MinION Mk1B.  Integrated
#          positions are always listed, even if (due to some hardware
#          error) they can't be found. They are never in
#          STATE_HARDWARE_REMOVED, and they always report a location.
#          Additionally, integrated positions cannot be associated
#          independently - if they are in STATE_NEEDS_ASSOCIATION, then
#          the host as a whole needs associating. Likewise, either all
#          integrated positions can run offline, or none of them can.
#          Since 4.4
#      can_sequence_offline:
#          Indicates that this position can sequence offline.  For
#          integrated positions, this is the same as the corresponding
#          field returned from the describe_host RPC.
#      protocol_state:
#          Indicates the state of the last or current protocol on the
#          flow cell position.  Since 5.0.
#      is_simulated:
#          Whether the device is simulated.  If this is true, there is no
#          physical device - MinKNOW is simulating it. If it is false,
#          MinKNOW will be acquiring data from a real device.  Since 5.2
#      device_type:
#          The type of the device.  Since 5.2
#      parent_name:
#          The name of the device this flow cell position is part of.
#          For an integrated position, this will be the host serial, as
#          returned by describe_host().  For a MinION Mk1B, this will be
#          the same as the `name` field.  For a P2 Solo, this will be the
#          name of the P2 Solo unit.  Since 5.3
        manager_pb2.FlowCellPosition(
            name=name0,
            state=manager_pb2.FlowCellPosition.State.STATE_RUNNING,
            rpc_ports=manager_pb2.FlowCellPosition.RpcPorts(secure=port0),
        ),
        manager_pb2.FlowCellPosition(
            name=name1,
            state=manager_pb2.FlowCellPosition.State.STATE_RUNNING,
            rpc_ports=manager_pb2.FlowCellPosition.RpcPorts(secure=port1),
        ),
        manager_pb2.FlowCellPosition(
            name=name2,
            state=manager_pb2.FlowCellPosition.State.STATE_RUNNING,
            rpc_ports=manager_pb2.FlowCellPosition.RpcPorts(secure=port2),
        ),
        manager_pb2.FlowCellPosition(
            name=name3,
            state=manager_pb2.FlowCellPosition.State.STATE_RUNNING,
            rpc_ports=manager_pb2.FlowCellPosition.RpcPorts(secure=port3),
        ),
        manager_pb2.FlowCellPosition(
            name=name4,
            state=manager_pb2.FlowCellPosition.State.STATE_INITIALISING
        ),
    ]

    with SequencingPositionServer(PositionInfo(position_name=name0),port=port0) as sequencing_position_0, SequencingPositionServer(PositionInfo(position_name=name1),port=port1) as sequencing_position_1, SequencingPositionServer(PositionInfo(position_name=name2),port=port2) as sequencing_position_2, SequencingPositionServer(PositionInfo(position_name=name3),port=port3) as sequencing_position_3, SequencingPositionServer(PositionInfo(position_name=name4),port=port4) as sequencing_position_4:

        with ManagerServer(positions=positions, port=minknowDefaultPort) as server:
            while True:
                sequencing_position_0.set_protocol_runs([TEST_PROTOCOL_WITH_ACQUISTIIONS])
                sequencing_position_0.set_acquisition_runs([TEST_ACQUISITION])
                sequencing_position_0.set_acquisition_output_statistics(TEST_ACQUISITION.run_id, generateTestAcquistionOutputStats())
                sequencing_position_0.set_duty_time_statistics(TEST_ACQUISITION.run_id, generateTestDutyTimeStats())
                sequencing_position_1.set_protocol_runs([TEST_PROTOCOL_WITH_ACQUISTIIONS])
                sequencing_position_1.set_acquisition_runs([TEST_ACQUISITION])
                sequencing_position_1.set_acquisition_output_statistics(TEST_ACQUISITION.run_id, generateTestAcquistionOutputStats())
                sequencing_position_1.set_duty_time_statistics(TEST_ACQUISITION.run_id, generateTestDutyTimeStats())
                sequencing_position_2.set_protocol_runs([TEST_PROTOCOL_WITH_ACQUISTIIONS])
                sequencing_position_2.set_acquisition_runs([TEST_ACQUISITION])
                sequencing_position_2.set_acquisition_output_statistics(TEST_ACQUISITION.run_id, generateTestAcquistionOutputStats())
                sequencing_position_2.set_duty_time_statistics(TEST_ACQUISITION.run_id, generateTestDutyTimeStats())
                sequencing_position_3.set_protocol_runs([TEST_PROTOCOL_WITH_ACQUISTIIONS])
                sequencing_position_3.set_acquisition_runs([TEST_ACQUISITION])
                sequencing_position_3.set_acquisition_output_statistics(TEST_ACQUISITION.run_id, generateTestAcquistionOutputStats())
                sequencing_position_3.set_duty_time_statistics(TEST_ACQUISITION.run_id, generateTestDutyTimeStats())
                sequencing_position_4.set_protocol_runs([TEST_PROTOCOL_WITH_ACQUISTIIONS])
                sequencing_position_4.set_acquisition_runs([TEST_ACQUISITION])
                sequencing_position_4.set_acquisition_output_statistics(TEST_ACQUISITION.run_id, generateTestAcquistionOutputStats())
                sequencing_position_4.set_duty_time_statistics(TEST_ACQUISITION.run_id, generateTestDutyTimeStats())
                time.sleep(1)

if __name__ == "__main__":
    mock()
