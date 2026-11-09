# BQ2 pipeline evidence: demo_run

- Execution: `arn:aws:states:us-east-1:536697253251:execution:is459-g2t1-pipeline:demo_run`
- Status: **SUCCEEDED**, started 2026-11-09 14:07:50.045000+00:00, stopped 2026-11-09 15:39:31.979000+00:00
- Built copy `bq2_sequences_b` while `a` was live
- BQ2 queries: 234 (110 build inserts), 337.60 GB scanned, about USD 1.69

## Gates

Each gate passes only if `ok` is true. Full result rows below.

**gate source before build**: ok = **true**, query `5c885614-0108-46bb-abab-75f008abef15`, 0.51 GB, 30s

| detail | source_fingerprint |
|---|---|
| Found HI: 31898218 rows, 31898218 ids, 28 days 2022-09-01 to 2022-09-28. LI: 31251469 rows, 31251469 ids, 27 days 2022-09-01 to 2022-09-27. Expected HI-Medium: 31898218 rows, 28 days 2022-09-01 to 2022-09-28. LI-Medium: 31251469 rows, 27 days 2022-09-01 to 2022-09-27. | 41345a9ae630e218da1bf82f2f8279819861ae338bab68efaa2ab398bdd89ae3 |

**gate count HI-Medium 2022-09-05**: ok = **true**, query `f122542f-56ec-48ee-88e5-02ec1e3df5f6`, 0.36 GB, 5s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-05 | 3661510 | 3661510 | 7511579 | 7511579 |

**gate count HI-Medium 2022-09-04**: ok = **true**, query `21b8689c-716a-4124-8808-e8d62380ed85`, 0.25 GB, 3s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-04 | 1065866 | 1065866 | 2081130 | 2081130 |

**gate count HI-Medium 2022-09-01**: ok = **true**, query `cbcedefb-c060-42a8-acce-36e8989f0958`, 0.68 GB, 6s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-01 | 14679196 | 14679196 | 36965485 | 36965485 |

**gate count HI-Medium 2022-09-03**: ok = **true**, query `e117c30d-97fe-4ed4-bd99-f45724de3218`, 0.18 GB, 3s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-03 | 650905 | 650905 | 774621 | 774621 |

**gate lineage HI-Medium 2022-09-05**: ok = **true**, query `c285e2ff-e698-4831-b6a2-9cdd2bb62180`, 0.41 GB, 6s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-05 | 29857757 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-04**: ok = **true**, query `860728cc-fa54-4353-bc68-1d69128bc48f`, 0.23 GB, 4s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-04 | 8375122 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-01**: ok = **true**, query `95870f78-c894-44e0-bea8-ab05e0b5a25d`, 1.18 GB, 9s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-01 | 140254847 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-03**: ok = **true**, query `e577ae66-6231-40f8-9f7d-1dca59cc29f7`, 0.16 GB, 3s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-03 | 3625673 | 0 | 0 | 0 |

**gate count HI-Medium 2022-09-06**: ok = **true**, query `a57c7753-f3e1-44bc-90bd-87ce5a536abf`, 0.36 GB, 4s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-06 | 3663642 | 3663642 | 7540378 | 7540378 |

**gate count HI-Medium 2022-09-07**: ok = **true**, query `a0b9833b-423c-4a0d-b888-2cd27d2248fc`, 0.37 GB, 4s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-07 | 3663264 | 3663264 | 7761545 | 7761545 |

**gate count HI-Medium 2022-09-08**: ok = **true**, query `a1f9acc2-7938-4092-9e9e-0eaf8f35444c`, 0.39 GB, 4s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-08 | 4024908 | 4024908 | 8524929 | 8524929 |

**gate lineage HI-Medium 2022-09-06**: ok = **true**, query `20bc71ff-6997-43f9-87a4-7134f2ebbc60`, 0.41 GB, 6s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-06 | 29948418 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-07**: ok = **true**, query `40a62c9f-90f2-4c27-aa2f-8ed8335539e3`, 0.44 GB, 6s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-07 | 30611163 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-08**: ok = **true**, query `f7cfcb4f-85ee-476c-a66b-706e4bb99560`, 0.42 GB, 6s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-08 | 33624603 | 0 | 0 | 0 |

**gate count HI-Medium 2022-09-10**: ok = **true**, query `e1be0276-309d-4c75-a43f-55feaccd0143`, 0.18 GB, 3s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-10 | 653014 | 653014 | 763634 | 763634 |

**gate count HI-Medium 2022-09-12**: ok = **true**, query `2a8ed326-6ed8-4cd8-85c7-6a7c243aedd3`, 0.36 GB, 9s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-12 | 3663034 | 3663034 | 7518970 | 7518970 |

**gate count HI-Medium 2022-09-11**: ok = **true**, query `12b557a5-384f-4bf8-816c-a45dd8489e68`, 0.25 GB, 5s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-11 | 1063242 | 1063242 | 2067487 | 2067487 |

**gate lineage HI-Medium 2022-09-10**: ok = **true**, query `4b8a3f7b-f020-4627-8bd1-1d1341c10a38`, 0.16 GB, 3s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-10 | 3596930 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-12**: ok = **true**, query `d7034e06-413b-41d9-bf97-9c1d19bbabc3`, 0.41 GB, 6s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-12 | 29882978 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-11**: ok = **true**, query `1cb92727-4793-44bc-b6f4-f964421b3300`, 0.23 GB, 4s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-11 | 8328945 | 0 | 0 | 0 |

**gate count HI-Medium 2022-09-13**: ok = **true**, query `0e1031c3-25d3-4d46-8f5b-e6c5247d67f9`, 0.36 GB, 4s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-13 | 3668860 | 3668860 | 7554087 | 7554087 |

**gate count HI-Medium 2022-09-14**: ok = **true**, query `cabec5b8-6d15-4b9f-809c-92536e2ae014`, 0.38 GB, 9s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-14 | 3669050 | 3669050 | 8047008 | 8047008 |

**gate count HI-Medium 2022-09-15**: ok = **true**, query `62a16b9d-87ff-4baa-a333-11afac7967c9`, 0.40 GB, 6s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-15 | 4399512 | 4399512 | 9809982 | 9809982 |

**gate count HI-Medium 2022-09-02**: ok = **true**, query `68abfcf6-8a3b-44ed-aa52-cf2bc82fbc11`, 0.38 GB, 29s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-02 | 232479164 | 232479164 | 289522967 | 289522967 |

**gate lineage HI-Medium 2022-09-14**: ok = **true**, query `26a1f438-b3e4-4542-970c-6d0e670bd4a9`, 0.46 GB, 7s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-14 | 31479124 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-13**: ok = **true**, query `9caa938f-00ed-4bb6-bced-d73b5ca9fb2f`, 0.41 GB, 5s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-13 | 29999981 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-15**: ok = **true**, query `132d8ff0-aa90-4f4a-97d2-31b1862e4158`, 0.42 GB, 8s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-15 | 38228970 | 0 | 0 | 0 |

**gate count HI-Medium 2022-09-09**: ok = **true**, query `7bf25266-e5e2-4551-a453-644f19346114`, 0.34 GB, 26s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-09 | 203178868 | 203178868 | 228693342 | 228693342 |

**gate lineage HI-Medium 2022-09-02**: ok = **true**, query `d9fda2ba-d5b0-4867-83e2-452c7b7ad1c5`, 7.59 GB, 67s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-02 | 1333527229 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-09**: ok = **true**, query `84be0d79-0238-4d3d-b789-6a7f1b7a2f04`, 6.09 GB, 53s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-09 | 1092437762 | 0 | 0 | 0 |

**gate count HI-Medium 2022-09-17**: ok = **true**, query `7a3b5a21-2d43-4e8b-a0a6-f0fa7a7ac4d9`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-17 | 1137 | 1137 | 1434 | 1434 |

**gate count HI-Medium 2022-09-18**: ok = **true**, query `5f288d29-18cd-47ea-baa4-151b725d3b07`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-18 | 765 | 765 | 792 | 792 |

**gate lineage HI-Medium 2022-09-17**: ok = **true**, query `86467262-234c-479f-af2e-bd466cba1059`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-17 | 6576 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-18**: ok = **true**, query `61c6541e-c801-47cb-bbe5-5ea88f1602d2`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-18 | 3906 | 0 | 0 | 0 |

**gate count HI-Medium 2022-09-19**: ok = **true**, query `5260ae8b-025f-43c9-8db9-8d71f531561b`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-19 | 577 | 577 | 475 | 475 |

**gate count HI-Medium 2022-09-20**: ok = **true**, query `cc8b2899-8287-42d1-9f30-0778bf561f86`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-20 | 483 | 483 | 453 | 453 |

**gate lineage HI-Medium 2022-09-19**: ok = **true**, query `932b3e72-cb6e-4c21-bce9-dcd6d23b3e0b`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-19 | 2579 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-20**: ok = **true**, query `0138ae66-762d-4664-837b-c21cde8a7404`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-20 | 2325 | 0 | 0 | 0 |

**gate count HI-Medium 2022-09-21**: ok = **true**, query `3c0494a6-72c4-4630-8b1e-2e7fd781f957`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-21 | 350 | 350 | 354 | 354 |

**gate count HI-Medium 2022-09-22**: ok = **true**, query `9aa56730-c24f-46ad-b82b-896ffe4fd6d4`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-22 | 124 | 124 | 71 | 71 |

**gate lineage HI-Medium 2022-09-21**: ok = **true**, query `b848011f-efc6-47f1-9186-9876c6bc3c3c`, 0.00 GB, 2s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-21 | 1762 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-22**: ok = **true**, query `db18e862-4415-4626-b718-5aef59f46a05`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-22 | 461 | 0 | 0 | 0 |

**gate count HI-Medium 2022-09-23**: ok = **true**, query `43ba6e9b-d67c-4780-a671-d8ed8a66f368`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-23 | 80 | 80 | 47 | 47 |

**gate count HI-Medium 2022-09-24**: ok = **true**, query `8381af2e-cd7e-43f8-9090-3f83de8a97d2`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-24 | 74 | 74 | 44 | 44 |

**gate lineage HI-Medium 2022-09-23**: ok = **true**, query `b5709bdb-9381-4b55-8197-e70e9487f625`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-23 | 301 | 0 | 0 | 0 |

**gate count HI-Medium 2022-09-16**: ok = **true**, query `82d47e04-7e9f-4d30-bf44-b334953aea63`, 0.31 GB, 36s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-16 | 180987982 | 180987982 | 161290580 | 161290580 |

**gate lineage HI-Medium 2022-09-24**: ok = **true**, query `8154a2a9-9a76-45a2-a40a-36def94e5647`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-24 | 280 | 0 | 0 | 0 |

**gate count HI-Medium 2022-09-25**: ok = **true**, query `0fdd4b88-d76c-4f08-b57f-ce7a4aa27cf3`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-25 | 104 | 104 | 103 | 103 |

**gate lineage HI-Medium 2022-09-16**: ok = **true**, query `e1dc656e-29b9-4320-b0dc-94ce4ff69f0e`, 4.39 GB, 45s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-16 | 845847704 | 0 | 0 | 0 |

**gate count HI-Medium 2022-09-26**: ok = **true**, query `8eeac252-aafe-4d66-8115-c246647a5273`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-26 | 49 | 49 | 26 | 26 |

**gate lineage HI-Medium 2022-09-25**: ok = **true**, query `3c8f25c3-33ac-4a43-a760-ed1f3b964e32`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-25 | 517 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-26**: ok = **true**, query `79184666-29cb-4b77-977d-49ae2f8b1f62`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-26 | 176 | 0 | 0 | 0 |

**gate count HI-Medium 2022-09-27**: ok = **true**, query `4e12a627-1269-441a-a3d0-741909b7904b`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-27 | 14 | 14 | 6 | 6 |

**gate count HI-Medium 2022-09-28**: ok = **true**, query `90f4e580-e4f8-4f23-a5e4-2e19b77e7a84`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-28 | 6 | 6 | 2 | 2 |

**gate count LI-Medium 2022-09-01**: ok = **true**, query `a7f81be6-f316-4d05-93d0-aaabf8e61cfb`, 0.67 GB, 5s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-01 | 14525100 | 14525100 | 36123744 | 36123744 |

**gate lineage HI-Medium 2022-09-27**: ok = **true**, query `60ecc227-77cf-4a3e-861d-05ee1703190d`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-27 | 46 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-28**: ok = **true**, query `b1a3381f-1e10-486c-826a-d1ca16c14e9a`, 0.00 GB, 2s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-28 | 18 | 0 | 0 | 0 |

**gate lineage LI-Medium 2022-09-01**: ok = **true**, query `eb42856d-2b3d-42a8-9603-3dd740ec62fc`, 1.16 GB, 9s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-01 | 137421432 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-03**: ok = **true**, query `d138c845-ed86-4683-a191-3cc9df2e04d9`, 0.17 GB, 5s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-03 | 638461 | 638461 | 750387 | 750387 |

**gate lineage LI-Medium 2022-09-03**: ok = **true**, query `87cab135-5f7c-4ac2-932e-374b811011ac`, 0.16 GB, 3s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-03 | 3528083 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-04**: ok = **true**, query `bdb91c6c-a292-4457-865e-88927263525c`, 0.24 GB, 8s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-04 | 1044013 | 1044013 | 2033160 | 2033160 |

**gate count LI-Medium 2022-09-05**: ok = **true**, query `33319448-9dd7-401d-80e9-f85bad9174e2`, 0.35 GB, 4s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-05 | 3587465 | 3587465 | 7373928 | 7373928 |

**gate count LI-Medium 2022-09-06**: ok = **true**, query `9001564c-df00-46eb-8150-b557ec94eee3`, 0.35 GB, 4s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-06 | 3587745 | 3587745 | 7382830 | 7382830 |

**gate lineage LI-Medium 2022-09-04**: ok = **true**, query `1abb1831-f7fd-4ba4-97b8-5c7670a839f8`, 0.23 GB, 4s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-04 | 8187506 | 0 | 0 | 0 |

**gate lineage LI-Medium 2022-09-05**: ok = **true**, query `6ec5794e-61d7-4295-8d44-0af13971bba3`, 0.40 GB, 7s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-05 | 29296714 | 0 | 0 | 0 |

**gate lineage LI-Medium 2022-09-06**: ok = **true**, query `4a273199-2bea-405e-9149-ffed089f7fa1`, 0.40 GB, 6s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-06 | 29323980 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-07**: ok = **true**, query `1552b127-238c-4b1d-b073-668b93763367`, 0.36 GB, 5s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-07 | 3582889 | 3582889 | 7631411 | 7631411 |

**gate lineage LI-Medium 2022-09-07**: ok = **true**, query `a5fafc9b-bc68-4d5e-87d9-e93f9b8fcb3b`, 0.43 GB, 7s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-07 | 30060011 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-08**: ok = **true**, query `a9366f0b-706c-454b-a01c-85fe4bbefc35`, 0.38 GB, 5s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-08 | 3945706 | 3945706 | 8330875 | 8330875 |

**gate lineage LI-Medium 2022-09-08**: ok = **true**, query `74637d5b-fa36-4492-b920-03c03e045dea`, 0.41 GB, 6s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-08 | 32884037 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-10**: ok = **true**, query `db8c9fac-7d58-44b8-92fa-c8ad1991ba10`, 0.17 GB, 10s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-10 | 635964 | 635964 | 756721 | 756721 |

**gate lineage LI-Medium 2022-09-10**: ok = **true**, query `2c5ed9f6-e6d8-4255-945a-fa3976d50d9f`, 0.16 GB, 3s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-10 | 3542091 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-11**: ok = **true**, query `bbce50d6-a910-4e82-a729-586b80156504`, 0.24 GB, 5s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-11 | 1044923 | 1044923 | 2031088 | 2031088 |

**gate lineage LI-Medium 2022-09-11**: ok = **true**, query `7525bd7a-bb44-4e8e-9a6f-e3c21a65d52e`, 0.23 GB, 5s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-11 | 8183110 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-12**: ok = **true**, query `4e451a11-7cc1-4731-91ed-6bd67918ae47`, 0.35 GB, 7s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-12 | 3586787 | 3586787 | 7366801 | 7366801 |

**gate lineage LI-Medium 2022-09-12**: ok = **true**, query `81513ed1-4089-4227-89a5-d32afcea3e67`, 0.40 GB, 6s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-12 | 29273977 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-13**: ok = **true**, query `48b02d61-7a19-42a5-b03a-ae7ecd5e2471`, 0.35 GB, 5s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-13 | 3590922 | 3590922 | 7366395 | 7366395 |

**gate count LI-Medium 2022-09-02**: ok = **true**, query `c8dff68a-257e-4537-bdef-a5e3d891e725`, 0.38 GB, 34s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-02 | 226960242 | 226960242 | 283167146 | 283167146 |

**gate lineage LI-Medium 2022-09-13**: ok = **true**, query `f61750bd-597d-4ac7-8353-d999a21e7da6`, 0.40 GB, 6s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-13 | 29281029 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-14**: ok = **true**, query `1f154573-4e1f-49ac-85c1-a188be54d53b`, 0.37 GB, 4s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-14 | 3589924 | 3589924 | 7877841 | 7877841 |

**gate lineage LI-Medium 2022-09-02**: ok = **true**, query `8cde353f-8c04-435f-8df2-ae75e687305d`, 7.31 GB, 64s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-02 | 1303421922 | 0 | 0 | 0 |

**gate lineage LI-Medium 2022-09-14**: ok = **true**, query `324389af-49cf-48c2-9595-a4bcf0072131`, 0.45 GB, 6s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-14 | 30813371 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-15**: ok = **true**, query `f327bacc-82ad-4ac8-acd9-6fd386a60bff`, 0.39 GB, 6s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-15 | 4316632 | 4316632 | 9579169 | 9579169 |

**gate lineage LI-Medium 2022-09-15**: ok = **true**, query `81559bec-f8bc-4f88-aa4a-a30a1216a3e6`, 0.41 GB, 7s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-15 | 37370771 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-17**: ok = **true**, query `3036e6f1-978f-4081-a6ef-8617f0471c3f`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-17 | 186 | 186 | 252 | 252 |

**gate count LI-Medium 2022-09-09**: ok = **true**, query `83238baf-0d6c-45b3-ae38-f584a607d77a`, 0.34 GB, 26s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-09 | 201561578 | 201561578 | 227753010 | 227753010 |

**gate lineage LI-Medium 2022-09-17**: ok = **true**, query `c265d25f-6dda-43d0-bf60-0c158798f6e3`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-17 | 1128 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-18**: ok = **true**, query `33dfb29d-8919-457f-8790-bcce8c60964e`, 0.00 GB, 3s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-18 | 210 | 210 | 201 | 201 |

**gate lineage LI-Medium 2022-09-09**: ok = **true**, query `16972435-13a2-459f-8fd3-68996593934d`, 6.02 GB, 56s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-09 | 1086382186 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-19**: ok = **true**, query `9db6400b-353f-4974-a051-93399589da50`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-19 | 60 | 60 | 46 | 46 |

**gate lineage LI-Medium 2022-09-18**: ok = **true**, query `a2c373e8-248f-403a-9657-c20b5b64a577`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-18 | 1023 | 0 | 0 | 0 |

**gate lineage LI-Medium 2022-09-19**: ok = **true**, query `0d113fed-dc28-450f-a136-be386787b95b`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-19 | 258 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-20**: ok = **true**, query `8fbe4cfe-81d1-416e-8a51-d8235924d757`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-20 | 137 | 137 | 178 | 178 |

**gate count LI-Medium 2022-09-21**: ok = **true**, query `f4e879e7-788d-4e77-b93e-cce0ce6ddc60`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-21 | 54 | 54 | 56 | 56 |

**gate lineage LI-Medium 2022-09-20**: ok = **true**, query `087353e3-23f9-4a4f-bbe6-7558c552ee51`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-20 | 808 | 0 | 0 | 0 |

**gate lineage LI-Medium 2022-09-21**: ok = **true**, query `a1c6ee32-fd55-4d81-8684-ad03ac8e869c`, 0.00 GB, 2s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-21 | 276 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-22**: ok = **true**, query `f2730095-b2bd-4978-b315-9356cdb1519e`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-22 | 29 | 29 | 19 | 19 |

**gate count LI-Medium 2022-09-23**: ok = **true**, query `9243a4a4-77e0-49af-a673-645f83b3e15d`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-23 | 7 | 7 | 3 | 3 |

**gate lineage LI-Medium 2022-09-22**: ok = **true**, query `eb985e69-9102-4a14-ace8-a58271e9b1cd`, 0.00 GB, 2s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-22 | 115 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-16**: ok = **true**, query `a8b31f5f-b36f-4a8c-9f59-00eefb355c33`, 0.31 GB, 33s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-16 | 174912255 | 174912255 | 153557238 | 153557238 |

**gate lineage LI-Medium 2022-09-23**: ok = **true**, query `730af5e2-fde4-4211-beb0-b27933a8a573`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-23 | 23 | 0 | 0 | 0 |

**gate lineage LI-Medium 2022-09-16**: ok = **true**, query `abc4f0cf-a293-4084-87b1-6e2cac798da0`, 4.26 GB, 43s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-16 | 810496224 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-24**: ok = **true**, query `682f3d0a-6251-4782-91a4-aa151075c6b8`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-24 | 4 | 4 | 0 | 0 |

**gate count LI-Medium 2022-09-25**: ok = **true**, query `6dd0a6c0-1920-4ef3-a7fa-737cb49b6b08`, 0.00 GB, 5s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-25 | 2 | 2 | 0 | 0 |

**gate lineage LI-Medium 2022-09-24**: ok = **true**, query `dda07ca5-37a1-464b-bfb1-270630fa6789`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-24 | 8 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-26**: ok = **true**, query `a93cf775-908e-471e-aed3-da4d00c71e97`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-26 | 0 | 0 | 0 | 0 |

**gate lineage LI-Medium 2022-09-25**: ok = **true**, query `13581444-61ef-4fac-9977-aa3520e2ab27`, 0.00 GB, 2s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-25 | 4 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-27**: ok = **true**, query `a6f78006-f3d8-4747-b816-f09340cdd0d8`, 0.00 GB, 3s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-27 | 0 | 0 | 0 | 0 |

**gate lineage LI-Medium 2022-09-26**: ok = **true**, query `dd88b61d-078b-4f81-a382-e05ffc761028`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-26 | 0 | 0 | 0 | 0 |

**gate lineage LI-Medium 2022-09-27**: ok = **true**, query `fda4b751-bdfc-4305-9e47-99c5ced1aa6f`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-27 | 0 | 0 | 0 | 0 |

**gate structure bq2_sequences_b**: ok = **true**, query `199ef929-78ab-42dd-aaec-f3c1bb34e24b`, 247.56 GB, 84s

| total_rows | bad_rule_version | bad_length | bad_array_lengths | repeated_ids | bad_gap_values | gap_out_of_range | bad_duration | bad_final_id | bad_final_label | bad_label_values | bad_label_count | bad_any_laundering | bad_all_laundering | bad_method_string | bad_currency_strings | bad_start_date | bad_sequence_id | duplicate_sequence_ids |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2871801105 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

**gate source after build**: ok = **true**, query `71361591-a618-49b4-8923-53e5799dc4a8`, 0.51 GB, 28s

| detail | source_fingerprint |
|---|---|
| Found HI: 31898218 rows, 31898218 ids, 28 days 2022-09-01 to 2022-09-28. LI: 31251469 rows, 31251469 ids, 27 days 2022-09-01 to 2022-09-27. Expected HI-Medium: 31898218 rows, 28 days 2022-09-01 to 2022-09-28. LI-Medium: 31251469 rows, 27 days 2022-09-01 to 2022-09-27. | 41345a9ae630e218da1bf82f2f8279819861ae338bab68efaa2ab398bdd89ae3 |

**gate lift outputs**: ok = **true**, query `eb43310e-4937-4162-8c2c-b0697d1572ab`, 0.48 GB, 7s

| sequence_groups | lift_groups | lift_support_mismatches | lift_duplicate_ranks | flagged_rows | flagged_groups | expected_flagged_groups | flagged_duplicates | flagged_bad_ranks | tradeoff_rows | tradeoff_accounting_errors | robustness_rows | hi_top10_rows | sankey_mismatches | sankey_groups |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 4 | 12 | 0 | 0 | 51916347 | 12 | 12 | 0 | 0 | 48 | 0 | 60 | 60 | 0 | 4 |

## Other BQ2 queries

| Label | Query id | GB scanned | Seconds |
|---|---|---|---|
| create bq2_sequences_b | `bf116ae6-e6b0-4097-b800-8e8ea7250d1f` | 0.00 | 1 |
| build HI-Medium 2022-09-01 pairs | `01edab18-9b81-41dc-9a5e-c562bab6c1f0` | 0.23 | 14 |
| build HI-Medium 2022-09-04 pairs | `8e715b2c-b18e-4fb5-8a21-a6a97bec7ba8` | 0.07 | 8 |
| build HI-Medium 2022-09-03 pairs | `4f097e7a-14ba-4ad1-8bca-bca4b6ead2c3` | 0.05 | 6 |
| build HI-Medium 2022-09-05 pairs | `8f04bcb8-f03e-43fa-94b3-df85be08e982` | 0.11 | 12 |
| build HI-Medium 2022-09-04 triples | `ce3230fa-f104-46d4-81b0-ae9b5afc7fe2` | 0.18 | 10 |
| build HI-Medium 2022-09-05 triples | `3e803679-cc76-4d52-93c0-66999e0d5c80` | 0.26 | 16 |
| build HI-Medium 2022-09-01 triples | `060c60d9-ed49-4e06-9ab9-b679e4c40c7d` | 0.47 | 32 |
| build HI-Medium 2022-09-03 triples | `5953cdac-8a39-4a77-85f2-2f9cf962fbe9` | 0.13 | 7 |
| build HI-Medium 2022-09-06 pairs | `c9ca2196-eb00-4fef-b0ea-b1c18d6f8ac9` | 0.11 | 11 |
| build HI-Medium 2022-09-08 pairs | `154f1ced-03d2-4597-8486-73523a2ac180` | 0.13 | 11 |
| build HI-Medium 2022-09-07 pairs | `f5c5ac11-804a-4d73-b670-3ee5fbbf24ac` | 0.11 | 11 |
| build HI-Medium 2022-09-06 triples | `34044a0a-f428-47a0-bf96-e7038204b522` | 0.26 | 17 |
| build HI-Medium 2022-09-08 triples | `3d5b6358-fa45-4ffc-921c-bba70b788294` | 0.27 | 14 |
| build HI-Medium 2022-09-07 triples | `de8f1b66-50a2-4b86-b69b-48ad9cbc92a0` | 0.27 | 13 |
| build HI-Medium 2022-09-02 pairs | `80ccc694-994d-4f2c-b64d-b3f0a1f63854` | 0.13 | 376 |
| build HI-Medium 2022-09-10 pairs | `6f6f10d8-00e3-42b7-932b-9cf9a1f9d18c` | 0.05 | 7 |
| build HI-Medium 2022-09-11 pairs | `671ff2ff-8c4b-493d-a2ee-a028df5704d6` | 0.07 | 10 |
| build HI-Medium 2022-09-12 pairs | `b7c70316-ef81-4f2a-84a7-57dfc23fb0ba` | 0.11 | 11 |
| build HI-Medium 2022-09-10 triples | `89353fa6-b4c9-49b0-9d74-62b3a98bbfb6` | 0.13 | 6 |
| build HI-Medium 2022-09-12 triples | `9e52a8ea-9b07-44b4-9f56-bfecafddf700` | 0.26 | 15 |
| build HI-Medium 2022-09-11 triples | `35c22945-5193-461d-a2d5-5115b514355f` | 0.18 | 9 |
| build HI-Medium 2022-09-09 pairs | `72f7cc68-9f19-4d3f-b13f-2819411177c9` | 0.12 | 331 |
| build HI-Medium 2022-09-13 pairs | `11006672-91ef-4014-a980-0ea7734ece71` | 0.11 | 11 |
| build HI-Medium 2022-09-14 pairs | `eb81f287-1acd-4d91-bd23-d1eb99ff9307` | 0.11 | 11 |
| build HI-Medium 2022-09-15 pairs | `299cb11a-f077-4146-a73e-5bf4855fa336` | 0.13 | 13 |
| build HI-Medium 2022-09-13 triples | `ce3fe1b3-fcce-40af-b0e3-b01a62b0529b` | 0.26 | 17 |
| build HI-Medium 2022-09-14 triples | `7c0a1e9d-e4df-4ec7-9197-567e0b9196cd` | 0.28 | 13 |
| build HI-Medium 2022-09-15 triples | `46d7ea37-6a4f-426e-84b8-8384f9fdb0db` | 0.28 | 15 |
| build HI-Medium 2022-09-02 triples | `b0efbe0e-8d2f-4757-810d-d2588238144c` | 0.26 | 410 |
| build HI-Medium 2022-09-09 triples | `1846090f-cc16-4e80-8ba4-49be67de66ba` | 0.23 | 308 |
| build HI-Medium 2022-09-17 pairs | `60fdb519-5e7b-4848-8940-45bb173f3558` | 0.00 | 2 |
| build HI-Medium 2022-09-18 pairs | `8d201b31-718e-4608-8831-a6fa056df927` | 0.00 | 2 |
| build HI-Medium 2022-09-17 triples | `340b69d7-08d3-4c65-8096-caae34e3a54d` | 0.00 | 2 |
| build HI-Medium 2022-09-18 triples | `ca8548b9-2097-4aa2-8ae3-56ac30999656` | 0.00 | 2 |
| build HI-Medium 2022-09-19 pairs | `3b6fae82-391e-426b-9ec7-2221e8238c40` | 0.00 | 2 |
| build HI-Medium 2022-09-20 pairs | `ae516aeb-13e1-41ee-886f-695b9c2e1763` | 0.00 | 2 |
| build HI-Medium 2022-09-19 triples | `38461663-7c70-4169-b03b-6a74f69f4a86` | 0.00 | 4 |
| build HI-Medium 2022-09-20 triples | `1302d4d7-f516-4fa5-9523-08213691fddf` | 0.00 | 2 |
| build HI-Medium 2022-09-21 pairs | `e5d25338-9422-4253-9c3d-34c22fe6533f` | 0.00 | 3 |
| build HI-Medium 2022-09-16 pairs | `bc6cb552-3bf2-46ad-ac45-021b56c588e2` | 0.12 | 294 |
| build HI-Medium 2022-09-22 pairs | `2e12bd74-d7d7-4cfe-8fbe-5a561e2a3d22` | 0.00 | 3 |
| build HI-Medium 2022-09-21 triples | `cef16497-b362-4bf0-95fe-ba0ef30d2614` | 0.00 | 3 |
| build HI-Medium 2022-09-22 triples | `307a64fb-ec9e-4e95-88e9-1567177633cf` | 0.00 | 2 |
| build HI-Medium 2022-09-23 pairs | `fc41350d-a601-49e7-abb4-63f2a83a612f` | 0.00 | 2 |
| build HI-Medium 2022-09-24 pairs | `1440b97b-f7c0-40cf-8eff-0f7427c43376` | 0.00 | 2 |
| build HI-Medium 2022-09-23 triples | `93a579c7-75ba-469d-a800-a0f0ca099659` | 0.00 | 3 |
| build HI-Medium 2022-09-24 triples | `5c23b885-2bfe-4e11-b7c3-53d2ac3e7c89` | 0.00 | 2 |
| build HI-Medium 2022-09-25 pairs | `8a7fcf18-570e-4405-af74-d0f747d765d0` | 0.00 | 2 |
| build HI-Medium 2022-09-16 triples | `4e0ab312-fb71-458d-b6e8-8eb341fd798b` | 0.20 | 222 |
| build HI-Medium 2022-09-26 pairs | `e3823566-d8f5-43cf-9255-6fa061b33014` | 0.00 | 2 |
| build HI-Medium 2022-09-25 triples | `8c530ede-77af-4f13-bd14-67c309f8a04d` | 0.00 | 2 |
| build HI-Medium 2022-09-26 triples | `f6980b0a-b5eb-4b7c-8747-e53f615c88e6` | 0.00 | 2 |
| build HI-Medium 2022-09-27 pairs | `7a02d083-cc7a-4381-ab61-0bafb3e6099c` | 0.00 | 2 |
| build HI-Medium 2022-09-28 pairs | `604aea18-4e5c-43c0-9d84-3ff6e8426437` | 0.00 | 2 |
| build HI-Medium 2022-09-27 triples | `4f7f438a-c3a1-442d-b6b4-b722237c425b` | 0.00 | 2 |
| build LI-Medium 2022-09-01 pairs | `eadd6bd0-9cbb-4259-b607-4b3419ab197e` | 0.23 | 18 |
| build HI-Medium 2022-09-28 triples | `5ff7cbc8-1773-43bd-98eb-dec67fac1149` | 0.00 | 2 |
| build LI-Medium 2022-09-01 triples | `ba915997-6b88-4690-8188-e6243ca4982c` | 0.46 | 31 |
| build LI-Medium 2022-09-03 pairs | `45322759-4cd4-49db-8fc3-91b6d1ada814` | 0.05 | 5 |
| build LI-Medium 2022-09-03 triples | `98bb1b93-e0a6-48dc-916e-6821f1ea21e6` | 0.13 | 6 |
| build LI-Medium 2022-09-04 pairs | `8c6078bb-73b7-4245-bca5-e5996c92ba13` | 0.07 | 8 |
| build LI-Medium 2022-09-05 pairs | `8941d56f-9f40-4f85-a75c-c0ab42502e55` | 0.11 | 11 |
| build LI-Medium 2022-09-06 pairs | `65bef914-5e45-4889-9749-6da9dcdf3d93` | 0.11 | 11 |
| build LI-Medium 2022-09-04 triples | `49bf1940-8ba5-487a-9979-8d95a155568d` | 0.18 | 9 |
| build LI-Medium 2022-09-05 triples | `b012b08b-a75c-45d6-a100-ec583bd57bc3` | 0.25 | 16 |
| build LI-Medium 2022-09-06 triples | `dbf5fa3b-bf17-46a9-a0c4-7c2b6b3a51d6` | 0.25 | 16 |
| build LI-Medium 2022-09-07 pairs | `c6a92807-e44a-4561-b8aa-7f7db9203354` | 0.11 | 11 |
| build LI-Medium 2022-09-07 triples | `4dcac4ec-9b13-4934-b1cb-fba78afe5a00` | 0.27 | 14 |
| build LI-Medium 2022-09-02 pairs | `4c9a1fe3-2663-4d40-b21c-df46958ea87e` | 0.13 | 403 |
| build LI-Medium 2022-09-08 pairs | `8391e794-75ab-48b9-9792-58bd8f22bfce` | 0.12 | 12 |
| build LI-Medium 2022-09-10 pairs | `531309b7-cab5-42b2-933b-ecc71b583670` | 0.05 | 7 |
| build LI-Medium 2022-09-08 triples | `87ccb586-ef6a-4dbd-b193-46dd61161f91` | 0.27 | 18 |
| build LI-Medium 2022-09-10 triples | `9852341e-b236-45fa-808b-d8f8995c70e2` | 0.13 | 6 |
| build LI-Medium 2022-09-11 pairs | `23dce4e5-41e8-4d0c-a330-3a04e2bcfee9` | 0.07 | 9 |
| build LI-Medium 2022-09-11 triples | `877fdf26-7a3b-470e-85bc-29ee3f7a53da` | 0.18 | 8 |
| build LI-Medium 2022-09-12 pairs | `236dbf0b-6868-4a38-b159-9587f6788033` | 0.11 | 12 |
| build LI-Medium 2022-09-09 pairs | `64ab8172-fff0-4e60-89ab-5b44fd2b673c` | 0.12 | 327 |
| build LI-Medium 2022-09-12 triples | `df6de511-7bb6-4060-940a-9b2f9c766064` | 0.25 | 17 |
| build LI-Medium 2022-09-13 pairs | `abb53a79-d69a-4abb-a8d9-ea32ed07a6b4` | 0.11 | 11 |
| build LI-Medium 2022-09-13 triples | `855ea9b0-f15e-458d-b4fe-66732bf28e17` | 0.25 | 17 |
| build LI-Medium 2022-09-14 pairs | `9fdeec96-476e-47cd-912e-4b36f90277c6` | 0.11 | 11 |
| build LI-Medium 2022-09-02 triples | `937a7289-c97c-41cd-b6c9-dc76e42c7c5f` | 0.25 | 393 |
| build LI-Medium 2022-09-14 triples | `222da096-91fa-418c-97a2-0219a2a41b92` | 0.27 | 14 |
| build LI-Medium 2022-09-15 pairs | `2d3e6688-2dcd-4641-b1e1-512abeff3e67` | 0.13 | 13 |
| build LI-Medium 2022-09-15 triples | `3ee6d94f-36cf-4349-8d34-a4d319c2f866` | 0.27 | 19 |
| build LI-Medium 2022-09-17 pairs | `37734ba2-8db2-447b-aa61-19f6cbd238cb` | 0.00 | 3 |
| build LI-Medium 2022-09-17 triples | `a4ae34d6-c1f7-4f59-b963-fca9cb248482` | 0.00 | 3 |
| build LI-Medium 2022-09-18 pairs | `914bca48-f3c7-4b3e-8dfc-8de9108e0873` | 0.00 | 3 |
| build LI-Medium 2022-09-09 triples | `c14f649c-c2c3-4b79-8fae-ac8e748830d7` | 0.23 | 324 |
| build LI-Medium 2022-09-19 pairs | `78873935-a5cd-4b3c-9817-34699b2de8f1` | 0.00 | 2 |
| build LI-Medium 2022-09-18 triples | `8fc1660f-31ec-43c1-90f4-a0ed4760aaa5` | 0.00 | 3 |
| build LI-Medium 2022-09-19 triples | `2ee8be07-a235-4040-8dcd-5ba0acb1a571` | 0.00 | 2 |
| build LI-Medium 2022-09-16 pairs | `5a38688d-9640-41c9-a36c-d8ba6b894a17` | 0.11 | 289 |
| build LI-Medium 2022-09-20 pairs | `9ba8bce5-5eeb-45c2-8098-41d2d5d2d80c` | 0.00 | 3 |
| build LI-Medium 2022-09-21 pairs | `145362fc-1313-40ed-b993-7509a02b63ee` | 0.00 | 4 |
| build LI-Medium 2022-09-20 triples | `82bd7788-6e7c-40c4-b362-58b2c2cae05e` | 0.00 | 2 |
| build LI-Medium 2022-09-22 pairs | `83151e91-fd6e-4b3e-ac08-674ed1e654eb` | 0.00 | 2 |
| build LI-Medium 2022-09-21 triples | `6521beb9-b0ec-4455-8c2d-16a4020195ed` | 0.00 | 2 |
| build LI-Medium 2022-09-23 pairs | `92a2ff69-a579-488b-a58f-c58e19485ea5` | 0.00 | 2 |
| build LI-Medium 2022-09-22 triples | `f88898d5-7c6d-4616-81a9-43d22dcaf13f` | 0.00 | 2 |
| build LI-Medium 2022-09-23 triples | `f992a757-6e76-4e53-9f8d-59a798c01956` | 0.00 | 2 |
| build LI-Medium 2022-09-16 triples | `b6f0f875-7dce-409d-bdfe-aca3e3239f7d` | 0.20 | 222 |
| build LI-Medium 2022-09-24 pairs | `f3a7affb-ca69-421f-80e7-e8153f57f1cb` | 0.00 | 2 |
| build LI-Medium 2022-09-25 pairs | `10a95213-fcb9-41dc-876a-c4fdc604c7e1` | 0.00 | 2 |
| build LI-Medium 2022-09-24 triples | `72b4abaf-2014-4454-9bb4-a1a251f6283e` | 0.00 | 2 |
| build LI-Medium 2022-09-25 triples | `acbf675f-a635-492e-930e-ac887d367d98` | 0.00 | 2 |
| build LI-Medium 2022-09-26 pairs | `8ce3d18d-67ba-43d4-9348-c4384e3e4bd9` | 0.00 | 2 |
| build LI-Medium 2022-09-27 pairs | `7a38cd44-743b-4cfa-ac22-9d233eea13d8` | 0.00 | 2 |
| build LI-Medium 2022-09-26 triples | `d9caf0bd-c39f-4b27-b6bd-d55c571d395b` | 0.00 | 2 |
| build LI-Medium 2022-09-27 triples | `f8f789ca-fee9-4496-94a6-77312bfe2e71` | 0.00 | 2 |
| publish bq2_sequences_b | `72da3de6-965b-4c3d-b047-ee4ca739d35b` | 0.00 | 1 |
| lift lift | `5a004591-9c23-454d-9d6d-ad4b6e0ed3ac` | 1.87 | 11 |
| lift flagged_hi2 | `3412eb4a-e322-4464-8fcb-4b0a61523538` | 5.29 | 6 |
| lift flagged_hi3 | `fb8659ed-a81c-4b45-9b7c-75694a6746d0` | 3.48 | 5 |
| lift flagged_li2 | `30c01cce-0685-4cd6-b50f-4b070f9a1ea7` | 5.19 | 5 |
| lift flagged_li3 | `ce6b9a10-fb17-4aa2-b447-a771cc6415ac` | 3.32 | 6 |
| lift tradeoff | `18ec388b-f311-4ba1-88f7-bdfca0884930` | 0.95 | 5 |
| lift robustness | `36fc45fe-42d4-427c-a961-8b2ffb56a434` | 0.00 | 1 |
| lift sankey | `92032823-cf6d-4e90-9c57-5c24438bdc0f` | 0.00 | 1 |
