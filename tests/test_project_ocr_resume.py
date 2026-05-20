from app.core.document_types import (
    DOCUMENT_TYPE_BUSINESS_BID,
    DOCUMENT_TYPE_TECHNICAL_BID,
)
from app.router import postgresql_batch as batch


def _payload() -> dict:
    return {
        "project": {
            "identifier_id": "project-uuid",
            "project_name": "resume-demo",
            "parsing_status": 0,
        },
        "documents": [
            {
                "relation_role": DOCUMENT_TYPE_BUSINESS_BID,
                "identifier_id": "business-done",
                "file_name": "business-done.pdf",
                "extracted": True,
                "tender_identifier_id": "tender-uuid",
                "tender_file_name": "tender.pdf",
                "tender_extracted": True,
            },
            {
                "relation_role": DOCUMENT_TYPE_BUSINESS_BID,
                "identifier_id": "business-pending",
                "file_name": "business-pending.pdf",
                "extracted": False,
                "tender_identifier_id": "tender-uuid",
                "tender_file_name": "tender.pdf",
                "tender_extracted": True,
            },
            {
                "relation_role": DOCUMENT_TYPE_TECHNICAL_BID,
                "identifier_id": "technical-pending",
                "file_name": "technical-pending.pdf",
                "extracted": False,
                "tender_identifier_id": "tender-uuid",
                "tender_file_name": "tender.pdf",
                "tender_extracted": True,
            },
        ],
    }


def test_ocr_stage_progress_marks_completed_and_pending_documents() -> None:
    stage_progress = batch._ocr_stage_progress(_payload())

    tender, business, technical = stage_progress
    assert tender["total_count"] == 1
    assert tender["completed_count"] == 1
    assert tender["pending_count"] == 0
    assert business["total_count"] == 2
    assert business["completed_count"] == 1
    assert business["pending_count"] == 1
    assert technical["total_count"] == 1
    assert technical["completed_count"] == 0
    assert technical["pending_count"] == 1


def test_planned_ocr_stages_resume_only_unextracted_documents() -> None:
    planned = batch._planned_ocr_stages(
        _payload(),
        current_status=0,
        target_stage=batch._OCR_STAGE_TECHNICAL,
    )

    assert [item["stage"] for item in planned] == [
        batch._OCR_STAGE_BUSINESS,
        batch._OCR_STAGE_TECHNICAL,
    ]
    assert planned[0]["pending_documents"] == [
        {
            "identifier_id": "business-pending",
            "file_name": "business-pending.pdf",
            "extracted": False,
        }
    ]
    assert batch._planned_completed_count(planned) == 1


def test_project_identifier_from_payload_uses_real_project_uuid() -> None:
    assert batch._project_identifier_from_payload(_payload(), "resume-demo") == "project-uuid"


def test_resume_target_stage_supports_business_aliases() -> None:
    assert batch._normalize_resume_target_stage("business") == batch._OCR_STAGE_BUSINESS
    assert batch._normalize_resume_target_stage("商务标") == batch._OCR_STAGE_BUSINESS
