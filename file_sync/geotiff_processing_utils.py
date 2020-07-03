TILEGEN_JOB_TYPE = "tilegen"
PROCESSING_JOB_TYPE = "processing"


def get_job_config_map(media_path_details):
    return {
        TILEGEN_JOB_TYPE: media_path_details,
        PROCESSING_JOB_TYPE: media_path_details,
    }


def get_workflow_display_info(media_path_details):
    return {
        "title": media_path_details["dataset_name"] if "dataset_name" in media_path_details else "Dataset",
        "tiff_file_count": len(media_path_details["tiff_file_names"]) if "tiff_file_names" in media_path_details else -1,
    }
