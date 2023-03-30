# sudo kill $(ps aux | grep '9502' | awk '{print $2}')
from uuid import uuid4
import uuid
import logging
from pathlib import Path
from threading import Thread
import time
import grpc
import numpy as np
import sys
import time, random, argparse
from minknow_server import ManagerServer, SequencingPositionServer, FlowCellInfo, PositionInfo
from minknow_api import acquisition_pb2, manager_pb2, protocol_pb2, statistics_pb2
from minknow_api.manager import Manager
from minknow_api.manager_pb2 import SimulatedDeviceType
from minknow_api import Connection



import collections
import logging
import typing

import grpc

from minknow_api import protocol_pb2
from minknow_api.protocol_pb2 import BarcodeUserData




def find_protocol(
    device_connection: Connection,
    product_code: str,
    kit: str,
    basecalling: bool = False,
    basecall_config: typing.Optional[str] = None,
    barcoding: bool = False,
    barcoding_kits: typing.Optional[typing.List[str]] = None,
    force_reload: bool = False,
    experiment_type: str = "sequencing",
) -> typing.Optional[str]:
    """Find a protocol identifier.

    This will fetch a list of protocols from the device-instance, then search through the protocols
    for one that supports the flow-cell type (product code) and all the specified options. It
    returns the first protocol it finds that matches.

    Args:
        device_connection (:obj:`Connection`):  As returned by minknow.manager.FlowCellPosition().connect().
        product_code (:obj:`str`):              The flow-cell type, as in flow_cell_info.product_code.
        kit (:obj:`str`):                       The kit to be sequenced. eg: "SQK-LSK108".
        basecalling (bool):                     True if base-calling is required
        basecall_config (:obj:`str):            The base-calling model that the protocol should support. If absent,
                                                the protocol default will be used (if specified)
        barcoding (bool):                       True if barcoding is required.
        barcoding_kits (:obj:`list`):           Barcoding kits that the protocol should support. If specified,
                                                barcoding is assumed to be True.
        force_reload (bool):                    If true will force reloading the protocols from their descriptions,
                                                this will take a few seconds.
        experiment_type(:obj:`str`):            Type of experiment to be run.

    Returns:
        The first protocol to match or None.
    """

    try:
        response = device_connection.protocol.list_protocols(force_reload=force_reload)
    except grpc.RpcError as exception:
        raise Exception(
            "Could not get a list of protocols ({})".format(exception.details())
        )

    if not response.protocols:
        raise Exception("List of protocols is empty")

    for protocol in response.protocols:
        # we need the tags, if we don't have them move ont next protocol
        if not protocol.tag_extraction_result.success:
            LOGGER.debug("Ignoring protocol with tag extraction failure")
            continue

        # the tags provide a way to filter the experiments
        tags = protocol.tags

        # want a sequencing experiment...
        if experiment_type and tags["experiment type"].string_value != experiment_type:
            LOGGER.debug(
                "Ignoring experiment with incorrect type: %s vs %s",
                tags["experiment type"].string_value,
                experiment_type,
            )
            continue

        # ...for the correct flow-cell type...
        #if tags["flow cell"].string_value != product_code:
        #    LOGGER.debug(
        #        "Protocol is for product %s, not %s",
        #        tags["flow cell"].string_value,
        #        product_code,
        #    )
        #    continue

        # ...with matching kit
        #if tags["kit"].string_value != kit:
        #    LOGGER.debug(
        #        "Protocol supports kit %s, not %s", tags["kit"].string_value, kit
        #    )
        #    continue

        # if bar-coding is required, the protocol should support it and all
        # the bar-coding kits in use
        if tags["barcoding"].bool_value != barcoding:
            if barcoding:
                LOGGER.debug("Protocol does not support barcoding")
                continue
            else:
                if basecalling:
                    LOGGER.warning("Not using barcoding for a barcoding kit")
                LOGGER.debug("Protocol requires barcoding")

        supported_kits = tags["barcoding kits"].array_value
        # workaround for the set of barcoding kits being returned as a string rather
        # that array of strings
        if supported_kits and len(supported_kits[0]) == 1:
            supported_kits = (
                tags["barcoding kits"].array_value[1:-1].replace('"', "").split(",")
            )
        if tags["barcoding"].bool_value:
            supported_kits.append(tags["kit"].string_value)
        if barcoding_kits and not set(barcoding_kits).issubset(supported_kits):
            LOGGER.debug(
                "barcoding kits specified %s not amongst those supported %s",
                barcoding_kits,
                supported_kits,
            )
            continue

        # check base-calling is supported if required and with the requested model if
        # specified or default if not
        basecalling_required = basecalling or basecall_config is not None
        if basecalling_required:
            if not "default basecall model" in tags:
                continue
            default_basecall_model = tags.get("default basecall model").string_value
            if not basecall_config:
                basecall_config = default_basecall_model
            if basecall_config not in tags["available basecall models"].array_value:
                LOGGER.debug(
                    'basecalling model "%s" is not in the list of supported basecalling models %s',
                    basecall_config,
                    tags["available basecall models"].array_value,
                )
                continue

        # we have a match, (ignore the rest)
        return protocol
    return None


BarcodingArgs = collections.namedtuple(
    "BarcodingArgs",
    [
        "kits",
        "trim_barcodes",
        "barcodes_both_ends",
        "detect_mid_strand_barcodes",
        "min_score",
        "min_score_rear",
        "min_score_mid",
    ],
)
AlignmentArgs = collections.namedtuple("AlignmentArgs", ["reference_files", "bed_file"])
BasecallingArgs = collections.namedtuple(
    "BasecallingArgs", ["config", "barcoding", "alignment"]
)
OutputArgs = collections.namedtuple("OutputArgs", ["reads_per_file"])

ReadUntilArgs = collections.namedtuple(
    "ReadUntilArgs",
    [
        # "enrich", or "deplete"
        "filter_type",
        # List of reference files to pass to guppy for read until (only one file supported at the moment).
        "reference_files",
        # Bed file to pass to guppy for read until
        "bed_file",
        # First channel for read until to operate on.
        "first_channel",
        # Last channel for read until to operate on.
        "last_channel",
    ],
)


def make_protocol_arguments(
    experiment_duration: float = 72,
    basecalling: BasecallingArgs = None,
    read_until: ReadUntilArgs = None,
    fastq_arguments: OutputArgs = None,
    fast5_arguments: OutputArgs = None,
    bam_arguments: OutputArgs = None,
    disable_active_channel_selection: bool = False,
    mux_scan_period: float = 1.5,
    args: typing.Optional[typing.List[str]] = None,
    is_flongle: bool = False,
) -> typing.List[str]:
    """Build arguments to be used when starting a protocol.

    This will assemble the arguments passed to this script into arguments to pass to the protocol.

    Args:
        experiment_duration(float):             Length of the experiment in hours.
        basecalling(:obj:`BasecallingArgs`):    Arguments to control basecalling.
        read_until(:obj:`ReadUntilArgs):        Arguments to control read until.
        fastq_arguments(:obj:`OutputArgs`):     Control fastq file generation.
        fast5_arguments(:obj:`OutputArgs`):     Control fastq file generation.
        bam_arguments(:obj:`OutputArgs`):       Control bam file generation.
        disable_active_channel_selection(bool): Disable active channel selection
        mux_scan_period(float):                 Period of time between mux scans in hours.
        args(:obj:`list`):                      Extra arguments to pass to protocol.
        is_flongle(bool):                       Specify if the flow cell to be sequenced on is a flongle.

    Returns:
        A list of strings to be passed as arguments to start_protocol.
    """

    def on_off(value: bool):
        if value:
            return "on"
        else:
            return "off"

    protocol_args = []
    return protocol_args

    if basecalling:
        protocol_args.append("--base_calling=on")

        if basecalling.config:
            protocol_args.append("--guppy_filename=" + basecalling.config)

        if basecalling.barcoding:
            barcoding_args = []
            if basecalling.barcoding.kits:
                # list of barcoding kits converted to quoted, comma separated array elements
                # eg: barcoding_kits=['a','b','c']
                barcoding_args.append(
                    "barcoding_kits=['" + "','".join(basecalling.barcoding.kits) + "',]"
                )

            if basecalling.barcoding.trim_barcodes:
                # trim_barcodes=on/off
                barcoding_args.append(
                    "trim_barcodes=" + on_off(basecalling.barcoding.trim_barcodes)
                )

            if basecalling.barcoding.barcodes_both_ends:
                # require_barcodes_both_ends=on/off
                barcoding_args.append(
                    "require_barcodes_both_ends="
                    + on_off(basecalling.barcoding.barcodes_both_ends)
                )

            if basecalling.barcoding.detect_mid_strand_barcodes:
                # detect_mid_strand_barcodes=on/off
                barcoding_args.append(
                    "detect_mid_strand_barcodes="
                    + on_off(basecalling.barcoding.detect_mid_strand_barcodes)
                )

            if basecalling.barcoding.min_score:
                # min_score=66
                barcoding_args.append(
                    "min_score={}".format(basecalling.barcoding.min_score)
                )

            if basecalling.barcoding.min_score_rear:
                # min_score_rear=66
                barcoding_args.append(
                    "min_score_rear={}".format(basecalling.barcoding.min_score_rear)
                )

            if basecalling.barcoding.min_score_mid:
                # min_score_mid=66
                barcoding_args.append(
                    "min_score_mid={}".format(basecalling.barcoding.min_score_mid)
                )

            protocol_args.extend(["--barcoding"] + barcoding_args)

        if basecalling.alignment:
            alignment_args = []
            if basecalling.alignment.reference_files:
                alignment_args.append(
                    "reference_files=['"
                    + "','".join(basecalling.alignment.reference_files)
                    + "',]"
                )
            if basecalling.alignment.bed_file:
                alignment_args.append(
                    "bed_file='{}'".format(basecalling.alignment.bed_file)
                )
            protocol_args.extend(["--alignment"] + alignment_args)

    if read_until:
        read_until_args = []
        if read_until.filter_type:
            read_until_args.append("filter_type={}".format(read_until.filter_type))

        if read_until.reference_files:
            read_until_args.append(
                "reference_files=['" + "','".join(read_until.reference_files) + "',]"
            )

        if read_until.bed_file:
            read_until_args.append("bed_file='{}'".format(read_until.bed_file))

        if read_until.first_channel:
            read_until_args.append("first_channel={}".format(read_until.first_channel))

        if read_until.last_channel:
            read_until_args.append("last_channel={}".format(read_until.last_channel))

        # --read_until filter_type='enrich' reference_files=['/data/my-alignment-file'] bed_file='/data/bed_file.bed' first_channel=1 last_channel=512
        print(read_until_args)
        protocol_args.extend(["--read_until"] + read_until_args)

    protocol_args.append("--experiment_time={}".format(experiment_duration))
    protocol_args.append("--fast5=" + on_off(fast5_arguments))
    if fast5_arguments:
        protocol_args.extend(
            ["--fast5_data", "trace_table", "fastq", "raw", "vbz_compress"]
        )
        protocol_args.append(
            "--fast5_reads_per_file={}".format(fast5_arguments.reads_per_file)
        )

    protocol_args.append("--fastq=" + on_off(fastq_arguments))
    if fastq_arguments:
        protocol_args.extend(["--fastq_data", "compress"])
        protocol_args.append(
            "--fastq_reads_per_file={}".format(fastq_arguments.reads_per_file)
        )

    protocol_args.append("--bam=" + on_off(bam_arguments))
    if bam_arguments:
        if bam_arguments.reads_per_file != 4000:
            raise Exception("Unable to change reads per file for BAM.")
        """protocol_args.append(
            "--bam_reads_per_file={}".format(bam_arguments.reads_per_file)
        )"""

    if not is_flongle:
        protocol_args.append(
            "--active_channel_selection=" + on_off(not disable_active_channel_selection)
        )
        if not disable_active_channel_selection:
            protocol_args.append("--mux_scan_period={}".format(mux_scan_period))

    protocol_args.extend(args)

    return protocol_args


def start_protocol(
    device_connection: Connection,
    identifier: str,
    sample_id: str,
    experiment_group: str,
    barcode_info: typing.Optional[typing.Sequence[BarcodeUserData]],
    *args,
    **kwargs
) -> str:
    """Start a protocol on the passed {device_connection}.

    Args:
        device_connection(:obj:`Connection`):   The device connection to start a protocol on.
        identifier(str):                        Protocol identifier to be started.
        sample_id(str):                         Sample id of protocol to start.
        experiment_group(str):                  Experiment group of protocol to start.
        barcode_info(Sequence[:obj:`BarcodeUserData`]):
                Barcode user data (sample type and alias)
        *args: Additional arguments forwarded to {make_protocol_arguments}
        **kwargs: Additional arguments forwarded to {make_protocol_arguments}

    Returns:
        The protocol_run_id of the started protocol.
    """

    flow_cell_info = device_connection.device.get_flow_cell_info()

    protocol_arguments = make_protocol_arguments(
        *args, is_flongle=flow_cell_info.has_adapter, **kwargs
    )
    LOGGER.debug("Built protocol arguments: %s", " ".join(protocol_arguments))

    user_info = protocol_pb2.ProtocolRunUserInfo()
    if sample_id:
        user_info.sample_id.value = sample_id
    if experiment_group:
        user_info.protocol_group_id.value = experiment_group
    if barcode_info:
        user_info.barcode_user_info.extend(barcode_info)

    result = device_connection.protocol.start_protocol(
        identifier=identifier, args=protocol_arguments, user_info=user_info
    )

    return result














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

TEST_ACQUISITION = acquisition_pb2.AcquisitionRunInfo(run_id=str(uuid.uuid4()))

TEST_PROTOCOL = protocol_pb2.ProtocolRunInfo(
    run_id=str(uuid.uuid4()),
)
TEST_PROTOCOL_WITH_ACQUISTIIONS = protocol_pb2.ProtocolRunInfo(
    run_id=str(uuid.uuid4()), acquisition_run_ids=[TEST_ACQUISITION.run_id]
)

def generateTestAcquistionOutputStats():
    read_count = round((round(time.time())%(72*24*3600))/10) # single new read per 10 sec
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

def mock():
    """Entrypoint to extract run statistics example"""
    # Parse arguments to be passed to started protocols:
    parser = argparse.ArgumentParser(
        description="""
        Collect statistics from an existing protocol.
        """
    )

    parser.add_argument(
        "--host",
        default="localhost",
        help="IP address of the machine running MinKNOW (defaults to localhost)",
    )
    parser.add_argument(
        "--port",
        default=9502,
        help="Port to connect to on host (defaults to standard MinKNOW port)",
    )
    parser.add_argument(
        "--api-token",
        default=None,
        help="Specify an API token to use, should be returned from the sequencer as a developer API token.",
    )
    parser.add_argument(
        "--position",
        default=None,
        help="position on the machine (or MinION serial number) to run the protocol at",
    )
    parser.add_argument(
        "--protocol",
        help="Extract information for a specific protocol run-id (eg. 04462a44-eed3-4550-af0d-bc9683352583 returned form protocol.list_protocol_runs). Defaults to last run protocol.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    # Specify --verbose on the command line to get extra details about
    if args.verbose:
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    # Construct a manager using the host + port provided:
    manager = Manager(
        host=args.host, port=args.port, developer_api_token=args.api_token
    )
    
    #enum SimulatedDeviceType {
#    SIMULATED_AUTO = 0;
#    SIMULATED_MINION = 1;
#    SIMULATED_TRAXION = 2;
#    SIMULATED_PROMETHION = 3;
#}

    manager.add_simulated_device(name="MS12346", type=SimulatedDeviceType.SIMULATED_AUTO)
    time.sleep(10)
    positions = list(manager.flow_cell_positions())
    print(len(positions))
    print(positions)
    connection = positions[0].connect()

    #time.sleep(5)
    #protocols = connection.protocol.list_protocols().protocols
    #print(len(protocols))
    
    #connection.protocol.start_protocol(identifier=protocols[0])
    time.sleep(5)

    #manager.remove_simulated_device(name="MS12345")
    
    
    protocol = find_protocol(device_connection=connection, product_code='', kit='')
    time.sleep(10)
    
    print(protocol)
    
    result = start_protocol(device_connection=connection, identifier='sequencing/sequencing_MIN106_RNA:FLO-MIN106:SQK-RNA002', sample_id='', experiment_group='', barcode_info='')
    time.sleep(10)
    print(result.run_id)
    print(result.acquisition_run_ids)
    print('asd')
    time.sleep(1000)
    manager.remove_simulated_device(name="MS12345")
    #positions = list(manager.flow_cell_positions())
    #print(len(positions))

    #with ManagerServer(port=minknowDefaultPort) as server:
    #    server.add_simulated_device(self, name, type)
    #channel = GrpcChannel.ForAddress('127.0.0.1' + ':' + '9502');
    #manager = MinknowApi.Manager.ManagerService.ManagerServiceClient(channel);

    #request = AddSimulatedDeviceRequest();
    #request.Name = "MS12345";
    #request.Type = SimulatedDeviceType.SimulatedAuto;
    #manager.add_simulated_device(request);


#    with SequencingPositionServer(PositionInfo(position_name=name0),port=port0) as sequencing_position_0, SequencingPositionServer(PositionInfo(position_name=name1),port=port1) as sequencing_position_1, SequencingPositionServer(PositionInfo(position_name=name2),port=port2) as sequencing_position_2, SequencingPositionServer(PositionInfo(position_name=name3),port=port3) as sequencing_position_3, SequencingPositionServer(PositionInfo(position_name=name4),port=port4) as sequencing_position_4:

#        with ManagerServer(positions=positions, port=minknowDefaultPort) as server:
#            while True:
#                sequencing_position_0.set_protocol_runs([TEST_PROTOCOL_WITH_ACQUISTIIONS])
#                time.sleep(1)

if __name__ == "__main__":
    mock()
