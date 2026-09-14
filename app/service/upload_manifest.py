"""Persist the expected upload set separately from the successfully bound documents."""


def make_upload_manifest(tender_file, bidders):
    files = [{"slot": "tender", "name": tender_file.filename, "company": "招标文件",
              "role": "tender", "status": "pending"}]
    groups = []
    for index, (company, business, technical) in enumerate(bidders, 1):
        slots = {}
        for role, file in (("business_bid", business), ("technical_bid", technical)):
            slot = f"{role}:{index}"
            files.append({"slot": slot, "name": file.filename, "company": company,
                          "role": role, "status": "pending"})
            slots[role] = slot
        groups.append({"company": company, **slots, "bound": False})
    return {"version": 1, "files": files, "groups": groups}


def upload_summary(manifest):
    if not isinstance(manifest, dict):
        return {"upload_complete": True, "upload_issues": []}
    files = manifest.get("files") or []
    groups = manifest.get("groups") or []
    pending = [dict(f) for f in files if f.get("status") != "uploaded" or not f.get("document_id")]
    pending_companies = {f.get("company") for f in pending}
    issues = pending + [
        {"company": g.get("company"), "status": "unbound", "name": "文档关联未完成"}
        for g in groups if not g.get("bound") and g.get("company") not in pending_companies
    ]
    return {"upload_complete": bool(files and groups) and not issues, "upload_issues": issues}
