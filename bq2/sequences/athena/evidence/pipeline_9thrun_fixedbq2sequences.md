# BQ2 pipeline evidence: 9thrun_fixedbq2sequences

- Execution: `arn:aws:states:us-east-1:536697253251:execution:is459-g2t1-pipeline:9thrun_fixedbq2sequences`
- Status: **SUCCEEDED**, started 2026-11-09 06:34:28.068000+00:00, stopped 2026-11-09 07:53:03.827000+00:00
- Built copy `bq2_sequences_b` while `a` was live
- BQ2 queries: 225 (110 build inserts), 316.94 GB scanned, about USD 1.58

## Gates

Each gate passes only if `ok` is true. Full result rows below.

**gate source before build**: ok = **true**, query `2dc5ffc8-5a7f-43d2-b427-ae6b501fbbc4`, 0.51 GB, 27s

| detail | source_fingerprint |
|---|---|
| Found HI: 31898218 rows, 31898218 ids, 28 days 2022-09-01 to 2022-09-28. LI: 31251469 rows, 31251469 ids, 27 days 2022-09-01 to 2022-09-27. Expected HI-Medium: 31898218 rows, 28 days 2022-09-01 to 2022-09-28. LI-Medium: 31251469 rows, 27 days 2022-09-01 to 2022-09-27. | 77b2f99208ac0efc2e0b49a5445f3f92bf8ce7c7deb69ffd61bef09d4dfc2d07 |

**gate count HI-Medium 2022-09-03**: ok = **true**, query `e58b5cb0-c1b9-4b71-8ba9-feed7b3541ab`, 0.18 GB, 7s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-03 | 650905 | 650905 | 774621 | 774621 |

**gate count HI-Medium 2022-09-05**: ok = **true**, query `0f65fc81-1b07-43f2-8d1c-a21de271ec11`, 0.36 GB, 5s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-05 | 3661510 | 3661510 | 7511579 | 7511579 |

**gate count HI-Medium 2022-09-01**: ok = **true**, query `a02f1070-f82e-4e16-8b04-9b0a22f54bdd`, 0.68 GB, 6s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-01 | 14679196 | 14679196 | 36965485 | 36965485 |

**gate count HI-Medium 2022-09-04**: ok = **true**, query `fb41e53a-3b9a-486a-8eec-8433245150c8`, 0.25 GB, 3s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-04 | 1065866 | 1065866 | 2081130 | 2081130 |

**gate lineage HI-Medium 2022-09-03**: ok = **true**, query `53d4db83-38ec-4734-8fc6-a11aafbbe7fc`, 0.16 GB, 3s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-03 | 3625673 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-05**: ok = **true**, query `5aee77e1-fb55-448f-b039-5b2da74da9df`, 0.41 GB, 7s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-05 | 29857757 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-01**: ok = **true**, query `a758e451-516a-42dc-b49f-2d55c8d8157a`, 1.18 GB, 9s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-01 | 140254847 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-04**: ok = **true**, query `190e7081-e004-4858-8439-037843677096`, 0.23 GB, 4s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-04 | 8375122 | 0 | 0 | 0 |

**gate count HI-Medium 2022-09-07**: ok = **true**, query `f8bce1f9-778a-4a4f-ad6f-20e15ec1afc3`, 0.37 GB, 5s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-07 | 3663264 | 3663264 | 7761545 | 7761545 |

**gate count HI-Medium 2022-09-06**: ok = **true**, query `2a2518b2-3663-43c6-85bd-6e4c1c66b0cc`, 0.36 GB, 5s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-06 | 3663642 | 3663642 | 7540378 | 7540378 |

**gate count HI-Medium 2022-09-08**: ok = **true**, query `41fe3864-848d-4b9c-b369-b0c0ca179aaf`, 0.39 GB, 4s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-08 | 4024908 | 4024908 | 8524929 | 8524929 |

**gate lineage HI-Medium 2022-09-07**: ok = **true**, query `380c6a7e-1b86-4338-be5b-eeddb9669ff1`, 0.44 GB, 6s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-07 | 30611163 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-06**: ok = **true**, query `ea89510f-ccee-4039-9ab6-8d07f67df93b`, 0.41 GB, 5s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-06 | 29948418 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-08**: ok = **true**, query `01c333d6-6ebd-41fd-bff3-28472b917cde`, 0.42 GB, 7s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-08 | 33624603 | 0 | 0 | 0 |

**gate count HI-Medium 2022-09-10**: ok = **true**, query `4650b7d7-99c0-42fe-a90f-46b095fb7b91`, 0.18 GB, 3s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-10 | 653014 | 653014 | 763634 | 763634 |

**gate count HI-Medium 2022-09-11**: ok = **true**, query `f36f038e-ad6f-4e2a-ade3-6a17bff1f942`, 0.25 GB, 5s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-11 | 1063242 | 1063242 | 2067487 | 2067487 |

**gate count HI-Medium 2022-09-12**: ok = **true**, query `1dccd29c-4280-459e-baae-34b2b8adfe64`, 0.36 GB, 4s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-12 | 3663034 | 3663034 | 7518970 | 7518970 |

**gate lineage HI-Medium 2022-09-10**: ok = **true**, query `8e7d1787-e372-406a-a158-861eac731126`, 0.16 GB, 3s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-10 | 3596930 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-11**: ok = **true**, query `049c6b30-3477-4c3b-9fd4-99b339196a5b`, 0.23 GB, 4s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-11 | 8328945 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-12**: ok = **true**, query `6cfbcb5d-d9f0-4c6d-a72d-c6ca120e3441`, 0.41 GB, 6s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-12 | 29882978 | 0 | 0 | 0 |

**gate count HI-Medium 2022-09-13**: ok = **true**, query `c8913f91-763c-42e1-9d95-6a0dfa38444b`, 0.36 GB, 8s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-13 | 3668860 | 3668860 | 7554087 | 7554087 |

**gate count HI-Medium 2022-09-14**: ok = **true**, query `629e8ad8-cb0c-451e-8a4a-2cffa22061fa`, 0.38 GB, 10s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-14 | 3669050 | 3669050 | 8047008 | 8047008 |

**gate count HI-Medium 2022-09-15**: ok = **true**, query `a6bfedf1-f893-4037-927f-47c0b6ee9435`, 0.40 GB, 5s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-15 | 4399512 | 4399512 | 9809982 | 9809982 |

**gate lineage HI-Medium 2022-09-13**: ok = **true**, query `5add1d7c-d563-420c-88a9-2f4461e0a238`, 0.41 GB, 7s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-13 | 29999981 | 0 | 0 | 0 |

**gate count HI-Medium 2022-09-02**: ok = **true**, query `ed0beb75-3710-4627-a155-092d346a2d83`, 0.38 GB, 29s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-02 | 232479164 | 232479164 | 289522967 | 289522967 |

**gate lineage HI-Medium 2022-09-14**: ok = **true**, query `0bb0eed8-f49e-4982-ba5f-334414de0123`, 0.46 GB, 6s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-14 | 31479124 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-15**: ok = **true**, query `49899762-0ca4-41bf-be6e-924e53c0ffe5`, 0.42 GB, 7s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-15 | 38228970 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-02**: ok = **true**, query `a0184830-5f64-4aad-9580-595acdbbbbfa`, 7.60 GB, 69s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-02 | 1333527229 | 0 | 0 | 0 |

**gate count HI-Medium 2022-09-09**: ok = **true**, query `57e1c804-be4d-40c4-8070-32be7abc4fab`, 0.34 GB, 25s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-09 | 203178868 | 203178868 | 228693342 | 228693342 |

**gate count HI-Medium 2022-09-17**: ok = **true**, query `eea05629-2946-4eea-bf6d-930acd6b37b0`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-17 | 1137 | 1137 | 1434 | 1434 |

**gate count HI-Medium 2022-09-18**: ok = **true**, query `e55c9624-a2b0-4669-9114-e3576fa9ddb3`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-18 | 765 | 765 | 792 | 792 |

**gate lineage HI-Medium 2022-09-09**: ok = **true**, query `28872839-b9e4-4624-91ec-ad926333113c`, 6.06 GB, 56s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-09 | 1092437762 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-17**: ok = **true**, query `0e27bf58-a10b-41ec-86c9-ae5ab5bad94c`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-17 | 6576 | 0 | 0 | 0 |

**gate count HI-Medium 2022-09-19**: ok = **true**, query `17e1dc74-2e21-431d-b02a-796555fd4c14`, 0.00 GB, 3s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-19 | 577 | 577 | 475 | 475 |

**gate lineage HI-Medium 2022-09-18**: ok = **true**, query `0a947e40-118a-4cda-bfc2-2cfa1319e0b4`, 0.00 GB, 2s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-18 | 3906 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-19**: ok = **true**, query `6c93bf6c-bc0e-4a35-a2f0-58c94963f80e`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-19 | 2579 | 0 | 0 | 0 |

**gate count HI-Medium 2022-09-20**: ok = **true**, query `ec76dbeb-15b1-4104-8b5e-070a91dec6e0`, 0.00 GB, 3s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-20 | 483 | 483 | 453 | 453 |

**gate count HI-Medium 2022-09-22**: ok = **true**, query `b1c750eb-7d95-498d-b056-7adf9de15d22`, 0.00 GB, 1s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-22 | 124 | 124 | 71 | 71 |

**gate lineage HI-Medium 2022-09-20**: ok = **true**, query `379bda21-7a2d-41f3-824a-009e5d945461`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-20 | 2325 | 0 | 0 | 0 |

**gate count HI-Medium 2022-09-21**: ok = **true**, query `10a7b5b2-88e6-4443-b4d7-108f873d85de`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-21 | 350 | 350 | 354 | 354 |

**gate count HI-Medium 2022-09-23**: ok = **true**, query `8698d7ce-19e5-4610-8c3d-083856a6222b`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-23 | 80 | 80 | 47 | 47 |

**gate lineage HI-Medium 2022-09-22**: ok = **true**, query `79bd37e1-5d51-412f-8420-4a1f42b91b07`, 0.00 GB, 2s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-22 | 461 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-21**: ok = **true**, query `597c2f9a-164c-41dd-88a0-7fd07e86e72a`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-21 | 1762 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-23**: ok = **true**, query `4eaa66bf-6290-4541-980e-f94cc35cbff9`, 0.00 GB, 2s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-23 | 301 | 0 | 0 | 0 |

**gate count HI-Medium 2022-09-16**: ok = **true**, query `4a70ce3d-cd4a-4ddd-9638-a8100d91d47c`, 0.31 GB, 29s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-16 | 180987982 | 180987982 | 161290580 | 161290580 |

**gate count HI-Medium 2022-09-24**: ok = **true**, query `b227d503-60d5-4d3a-9072-877e035c7a8c`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-24 | 74 | 74 | 44 | 44 |

**gate count HI-Medium 2022-09-26**: ok = **true**, query `bb3a86b2-042e-4f60-87e7-385b532fb35e`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-26 | 49 | 49 | 26 | 26 |

**gate lineage HI-Medium 2022-09-16**: ok = **true**, query `90ad2173-c420-485d-983e-3ecaed9f6606`, 4.37 GB, 45s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-16 | 845847704 | 0 | 0 | 0 |

**gate count HI-Medium 2022-09-25**: ok = **true**, query `47915cd7-aeac-4652-b1e6-42689981fdd5`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-25 | 104 | 104 | 103 | 103 |

**gate lineage HI-Medium 2022-09-24**: ok = **true**, query `f6d347b7-48f9-4961-9b78-c4af9e624e7a`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-24 | 280 | 0 | 0 | 0 |

**gate count HI-Medium 2022-09-27**: ok = **true**, query `430730e7-5d38-45ef-a359-dde9bfd20712`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-27 | 14 | 14 | 6 | 6 |

**gate lineage HI-Medium 2022-09-26**: ok = **true**, query `5f981a9d-7893-4d60-9f36-4bf0622c906d`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-26 | 176 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-25**: ok = **true**, query `31927983-f6e8-4028-b037-e75fa6468eef`, 0.00 GB, 3s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-25 | 517 | 0 | 0 | 0 |

**gate lineage HI-Medium 2022-09-27**: ok = **true**, query `fb4d76c9-49d8-4edc-b2a4-593bec82ee3c`, 0.00 GB, 2s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-27 | 46 | 0 | 0 | 0 |

**gate count HI-Medium 2022-09-28**: ok = **true**, query `32d0e995-e693-4cd8-9923-b102cf355ea7`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-28 | 6 | 6 | 2 | 2 |

**gate count LI-Medium 2022-09-01**: ok = **true**, query `0f926954-f3ab-435e-9c25-a1294978936b`, 0.67 GB, 6s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-01 | 14525100 | 14525100 | 36123744 | 36123744 |

**gate count LI-Medium 2022-09-03**: ok = **true**, query `580c0a86-74ec-4630-a653-aa9a4f7a963d`, 0.17 GB, 3s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-03 | 638461 | 638461 | 750387 | 750387 |

**gate lineage HI-Medium 2022-09-28**: ok = **true**, query `c3db2797-7843-4645-8fec-66ccb43745f7`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| HI-Medium | 2022-09-28 | 18 | 0 | 0 | 0 |

**gate lineage LI-Medium 2022-09-01**: ok = **true**, query `5c5d7009-9bdf-4792-86b5-a5bc43a57955`, 1.16 GB, 12s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-01 | 137421432 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-04**: ok = **true**, query `eee8392c-3adb-41c8-9f3f-bfe8474b4d9e`, 0.24 GB, 4s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-04 | 1044013 | 1044013 | 2033160 | 2033160 |

**gate lineage LI-Medium 2022-09-03**: ok = **true**, query `514d76bd-4231-48fb-a9f0-4201c1532824`, 0.16 GB, 3s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-03 | 3528083 | 0 | 0 | 0 |

**gate lineage LI-Medium 2022-09-04**: ok = **true**, query `d8700be5-5b8d-4b01-9f23-a3a6f8486fc9`, 0.23 GB, 4s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-04 | 8187506 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-06**: ok = **true**, query `8138bf8f-0e3a-4b0f-99a0-1833e57ac2fe`, 0.35 GB, 4s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-06 | 3587745 | 3587745 | 7382830 | 7382830 |

**gate count LI-Medium 2022-09-05**: ok = **true**, query `e820bc33-6164-4544-a24a-6db88df11c28`, 0.35 GB, 5s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-05 | 3587465 | 3587465 | 7373928 | 7373928 |

**gate lineage LI-Medium 2022-09-06**: ok = **true**, query `2ab4487d-0d46-4e1b-9b5c-144761c2efe2`, 0.40 GB, 6s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-06 | 29323980 | 0 | 0 | 0 |

**gate lineage LI-Medium 2022-09-05**: ok = **true**, query `7f149a6d-85c6-4bb3-af7f-4bf5c9ccfa8e`, 0.40 GB, 6s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-05 | 29296714 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-07**: ok = **true**, query `757ccdd0-201f-4e56-9f95-e04f05c6ba13`, 0.36 GB, 4s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-07 | 3582889 | 3582889 | 7631411 | 7631411 |

**gate count LI-Medium 2022-09-08**: ok = **true**, query `75ec4687-273e-4cbe-a884-308afb5c902f`, 0.38 GB, 5s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-08 | 3945706 | 3945706 | 8330875 | 8330875 |

**gate lineage LI-Medium 2022-09-07**: ok = **true**, query `92f25a31-a9a2-41e3-8781-a9366ba62b13`, 0.43 GB, 6s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-07 | 30060011 | 0 | 0 | 0 |

**gate lineage LI-Medium 2022-09-08**: ok = **true**, query `c62ef187-afe4-40a1-87c5-6934d92dcde1`, 0.41 GB, 7s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-08 | 32884037 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-10**: ok = **true**, query `11d7fd5a-786f-46b8-ad5d-e0c1a06fe797`, 0.17 GB, 4s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-10 | 635964 | 635964 | 756721 | 756721 |

**gate lineage LI-Medium 2022-09-10**: ok = **true**, query `feac2ebe-d84c-4556-8c33-bab25be74055`, 0.16 GB, 4s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-10 | 3542091 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-11**: ok = **true**, query `1aaccf0d-5918-45f8-9909-3b01b9034011`, 0.24 GB, 3s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-11 | 1044923 | 1044923 | 2031088 | 2031088 |

**gate count LI-Medium 2022-09-12**: ok = **true**, query `4fcbdd6b-da8a-42ed-bd3e-7b97808d51ae`, 0.35 GB, 5s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-12 | 3586787 | 3586787 | 7366801 | 7366801 |

**gate lineage LI-Medium 2022-09-11**: ok = **true**, query `284bd124-d9e4-404e-b164-b0cc8d48d8f2`, 0.23 GB, 4s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-11 | 8183110 | 0 | 0 | 0 |

**gate lineage LI-Medium 2022-09-12**: ok = **true**, query `c45b9b2c-57f2-414c-b078-57979a1b8394`, 0.40 GB, 6s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-12 | 29273977 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-13**: ok = **true**, query `1610acf2-d9e2-4b01-8a5b-a885b6de06dc`, 0.35 GB, 4s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-13 | 3590922 | 3590922 | 7366395 | 7366395 |

**gate count LI-Medium 2022-09-02**: ok = **true**, query `df384765-7bfa-4958-b519-bfd75eea77c3`, 0.38 GB, 30s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-02 | 226960242 | 226960242 | 283167146 | 283167146 |

**gate lineage LI-Medium 2022-09-13**: ok = **true**, query `149dac87-1448-4fee-8dd9-6b68d284d521`, 0.40 GB, 6s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-13 | 29281029 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-14**: ok = **true**, query `99715070-3b84-4992-8180-f5004aa69d9a`, 0.37 GB, 9s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-14 | 3589924 | 3589924 | 7877841 | 7877841 |

**gate lineage LI-Medium 2022-09-14**: ok = **true**, query `d68625c3-0054-43a0-a4d9-f411361d25fc`, 0.45 GB, 6s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-14 | 30813371 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-15**: ok = **true**, query `441b5c9e-389c-46fd-8526-ac5d9664d3ab`, 0.39 GB, 5s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-15 | 4316632 | 4316632 | 9579169 | 9579169 |

**gate lineage LI-Medium 2022-09-02**: ok = **true**, query `47e23a5d-919d-47dd-b5ca-be15303980ee`, 7.32 GB, 67s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-02 | 1303421922 | 0 | 0 | 0 |

**gate lineage LI-Medium 2022-09-15**: ok = **true**, query `ebe9bb7c-2d6b-45e2-98e3-e4d150383284`, 0.41 GB, 6s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-15 | 37370771 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-17**: ok = **true**, query `bf53d37b-a591-4506-a261-3f853f1b4907`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-17 | 186 | 186 | 252 | 252 |

**gate count LI-Medium 2022-09-09**: ok = **true**, query `e9937d0d-1f0d-4f8b-bb22-d5065296ad5f`, 0.34 GB, 27s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-09 | 201561578 | 201561578 | 227753010 | 227753010 |

**gate lineage LI-Medium 2022-09-17**: ok = **true**, query `8638c669-d59d-4329-aa0c-df9142cf7f37`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-17 | 1128 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-18**: ok = **true**, query `8b6ea414-d051-4a97-9532-49f6ebdd7cf1`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-18 | 210 | 210 | 201 | 201 |

**gate count LI-Medium 2022-09-19**: ok = **true**, query `b61fe538-dacc-40ee-bfb6-a623f34bf4c4`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-19 | 60 | 60 | 46 | 46 |

**gate lineage LI-Medium 2022-09-18**: ok = **true**, query `58030694-7013-4c49-a0c6-cd806c654746`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-18 | 1023 | 0 | 0 | 0 |

**gate lineage LI-Medium 2022-09-19**: ok = **true**, query `389d4d5d-b240-4aaa-87cd-e5e92a5a37ad`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-19 | 258 | 0 | 0 | 0 |

**gate lineage LI-Medium 2022-09-09**: ok = **true**, query `d574b2c9-0f8c-4c33-bf5d-6a1438e7fa98`, 6.02 GB, 55s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-09 | 1086382186 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-20**: ok = **true**, query `51a3da39-4e91-4643-bf0d-d8865a9220f8`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-20 | 137 | 137 | 178 | 178 |

**gate lineage LI-Medium 2022-09-20**: ok = **true**, query `f5d912a7-013c-4491-bbd0-57c776e8cae3`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-20 | 808 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-21**: ok = **true**, query `e40350ab-5d76-4bca-968a-181c7a0790b5`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-21 | 54 | 54 | 56 | 56 |

**gate count LI-Medium 2022-09-22**: ok = **true**, query `1cc5586e-266a-4c59-adb2-ebf130d12fa2`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-22 | 29 | 29 | 19 | 19 |

**gate count LI-Medium 2022-09-23**: ok = **true**, query `30cb6872-e5ca-4454-995b-52f1f8d47e1c`, 0.00 GB, 3s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-23 | 7 | 7 | 3 | 3 |

**gate lineage LI-Medium 2022-09-21**: ok = **true**, query `4d979c15-f0e3-4cb0-af95-c9162eab2234`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-21 | 276 | 0 | 0 | 0 |

**gate lineage LI-Medium 2022-09-22**: ok = **true**, query `26fd6953-179f-4833-afde-dd1215257985`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-22 | 115 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-16**: ok = **true**, query `087d2883-1999-4717-980a-028a7e4b7842`, 0.31 GB, 28s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-16 | 174912255 | 174912255 | 153557238 | 153557238 |

**gate lineage LI-Medium 2022-09-23**: ok = **true**, query `0f9950a0-0d1a-4cbc-84d7-6fbdcf786f35`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-23 | 23 | 0 | 0 | 0 |

**gate lineage LI-Medium 2022-09-16**: ok = **true**, query `93aa617e-58e8-420c-969d-05ae40987239`, 4.25 GB, 42s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-16 | 810496224 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-24**: ok = **true**, query `3eb7b38f-46b0-4a8c-9c64-f4254d890f91`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-24 | 4 | 4 | 0 | 0 |

**gate lineage LI-Medium 2022-09-24**: ok = **true**, query `ea63b2ec-a360-422f-a1c7-ab7524e927dc`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-24 | 8 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-25**: ok = **true**, query `81c80b7a-7533-45f5-a48a-b3f97bd09fd1`, 0.00 GB, 1s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-25 | 2 | 2 | 0 | 0 |

**gate count LI-Medium 2022-09-26**: ok = **true**, query `f1ca4718-9dd8-409b-ba6b-8f63478dc00a`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-26 | 0 | 0 | 0 | 0 |

**gate count LI-Medium 2022-09-27**: ok = **true**, query `6d6ae875-24d6-4f47-99c2-ce219c443f81`, 0.00 GB, 2s

| dataset | start_date | expected_pairs | actual_pairs | expected_triples | actual_triples |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-27 | 0 | 0 | 0 | 0 |

**gate lineage LI-Medium 2022-09-25**: ok = **true**, query `f57a56f0-ccf3-4bb3-a0b2-00538c8c9a8e`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-25 | 4 | 0 | 0 | 0 |

**gate lineage LI-Medium 2022-09-26**: ok = **true**, query `db707201-8c70-4867-9b24-c7f653a55856`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-26 | 0 | 0 | 0 | 0 |

**gate lineage LI-Medium 2022-09-27**: ok = **true**, query `d8632c14-3074-4457-ba11-08d46507a9b9`, 0.00 GB, 1s

| dataset | start_date | members_checked | missing_members | attribute_mismatches | broken_links |
|---|---|---|---|---|---|
| LI-Medium | 2022-09-27 | 0 | 0 | 0 | 0 |

**gate structure bq2_sequences_b**: ok = **true**, query `f3b07941-db1a-40b0-b3c7-9b19e0e01d7f`, 247.52 GB, 86s

| total_rows | bad_rule_version | bad_length | bad_array_lengths | repeated_ids | bad_gap_values | gap_out_of_range | bad_duration | bad_final_id | bad_final_label | bad_label_values | bad_label_count | bad_any_laundering | bad_all_laundering | bad_method_string | bad_currency_strings | bad_start_date | bad_sequence_id | duplicate_sequence_ids |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2871801105 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

**gate source after build**: ok = **true**, query `54578259-4501-4188-9872-fdf72dd41824`, 0.51 GB, 30s

| detail | source_fingerprint |
|---|---|
| Found HI: 31898218 rows, 31898218 ids, 28 days 2022-09-01 to 2022-09-28. LI: 31251469 rows, 31251469 ids, 27 days 2022-09-01 to 2022-09-27. Expected HI-Medium: 31898218 rows, 28 days 2022-09-01 to 2022-09-28. LI-Medium: 31251469 rows, 27 days 2022-09-01 to 2022-09-27. | 77b2f99208ac0efc2e0b49a5445f3f92bf8ce7c7deb69ffd61bef09d4dfc2d07 |

## Other BQ2 queries

| Label | Query id | GB scanned | Seconds |
|---|---|---|---|
| create bq2_sequences_b | `ab56f5b2-87f2-42fe-9771-37205a95d57d` | 0.00 | 1 |
| build HI-Medium 2022-09-05 pairs | `037cd20d-6ed7-4a4a-8632-4903281664bb` | 0.11 | 11 |
| build HI-Medium 2022-09-03 pairs | `3210ed12-4ef4-4193-bf1a-b1b67bb72206` | 0.05 | 6 |
| build HI-Medium 2022-09-04 pairs | `f382c4f9-32f2-4204-9ddf-d2d52475592d` | 0.07 | 8 |
| build HI-Medium 2022-09-01 pairs | `ddb347cd-0d9e-4858-81f4-ed43105fe4fe` | 0.23 | 17 |
| build HI-Medium 2022-09-05 triples | `6d20a9b3-0c23-4d16-9fc4-0e1f6f9763e5` | 0.26 | 17 |
| build HI-Medium 2022-09-03 triples | `3e691487-8135-4438-9945-50201ec81869` | 0.13 | 6 |
| build HI-Medium 2022-09-01 triples | `b0dd9241-a7ff-4c39-b801-e0a46013d0cc` | 0.47 | 32 |
| build HI-Medium 2022-09-04 triples | `0e57bc99-8eeb-440d-95ec-52f999c7bdbe` | 0.18 | 7 |
| build HI-Medium 2022-09-07 pairs | `4a2147ce-246d-4ccd-9bca-292e418293ae` | 0.11 | 12 |
| build HI-Medium 2022-09-06 pairs | `a1e4b5b9-2ef4-4742-ba6a-e695cdf25c45` | 0.11 | 11 |
| build HI-Medium 2022-09-08 pairs | `ac421531-5df5-4bb1-b1f4-b304c081f164` | 0.13 | 13 |
| build HI-Medium 2022-09-07 triples | `f9b3d54b-7842-4bfe-bed2-e209529f42f5` | 0.27 | 15 |
| build HI-Medium 2022-09-06 triples | `b3d5507c-e08c-45de-b2d0-2c3744f3991d` | 0.26 | 17 |
| build HI-Medium 2022-09-08 triples | `dfa8dae9-cbcd-4787-b06e-a6d3e98fdbc4` | 0.27 | 14 |
| build HI-Medium 2022-09-02 pairs | `1ada8c42-ed4e-4296-b37c-dbc7b03a8e80` | 0.13 | 374 |
| build HI-Medium 2022-09-10 pairs | `d0e0191e-8005-42f7-a35f-4379c6059fd4` | 0.05 | 8 |
| build HI-Medium 2022-09-11 pairs | `37e738ac-5ab5-4826-8819-661c932f6971` | 0.07 | 10 |
| build HI-Medium 2022-09-12 pairs | `404daa60-266e-4cb1-a47b-87b8573349b9` | 0.11 | 11 |
| build HI-Medium 2022-09-10 triples | `808b878a-a037-461c-8ba5-c51826e2b38b` | 0.13 | 6 |
| build HI-Medium 2022-09-11 triples | `2400692a-f707-4ef9-8971-65ff298b492e` | 0.18 | 8 |
| build HI-Medium 2022-09-12 triples | `431ca5af-3087-4ace-82d9-ed351f720a4b` | 0.26 | 16 |
| build HI-Medium 2022-09-09 pairs | `8f10f237-bf13-4062-ae6b-4d721688ec9e` | 0.12 | 324 |
| build HI-Medium 2022-09-13 pairs | `4d16e68f-9700-4829-873d-4119052589a4` | 0.11 | 11 |
| build HI-Medium 2022-09-14 pairs | `ffb947b2-e627-4794-98ef-15087e1707f8` | 0.11 | 12 |
| build HI-Medium 2022-09-15 pairs | `6fcb8ad4-cf0d-4ca7-ad71-1d14b0286a86` | 0.13 | 13 |
| build HI-Medium 2022-09-13 triples | `1f6bd77a-a255-4543-bd31-dc4619857d34` | 0.26 | 16 |
| build HI-Medium 2022-09-14 triples | `169ce6e4-91b8-46d4-bbb5-674a85b394b4` | 0.28 | 14 |
| build HI-Medium 2022-09-02 triples | `56175247-42c6-4391-85df-f25246ea2776` | 0.26 | 423 |
| build HI-Medium 2022-09-15 triples | `8a13cca7-cb15-45fb-9f61-806ee1c53e01` | 0.28 | 16 |
| build HI-Medium 2022-09-09 triples | `e89a1f25-009b-4adf-96dc-fddb37a0eea8` | 0.23 | 328 |
| build HI-Medium 2022-09-17 pairs | `718d8a3a-9a05-4cc6-8680-51012c658e08` | 0.00 | 2 |
| build HI-Medium 2022-09-18 pairs | `bbe0b3c1-8810-4b85-89a6-cea52e999418` | 0.00 | 2 |
| build HI-Medium 2022-09-17 triples | `aaf409a4-20f8-4465-9d2c-d25f5651be46` | 0.00 | 2 |
| build HI-Medium 2022-09-19 pairs | `a0b708f0-61df-497c-a867-df34b78e4455` | 0.00 | 2 |
| build HI-Medium 2022-09-18 triples | `fd62c16a-dab9-4059-be6e-0a48e03b185d` | 0.00 | 5 |
| build HI-Medium 2022-09-19 triples | `788dab4d-5b55-4f49-acfe-9783da2d4e8d` | 0.00 | 2 |
| build HI-Medium 2022-09-20 pairs | `282bfeeb-5b68-40d3-90b0-e31836ccece3` | 0.00 | 2 |
| build HI-Medium 2022-09-21 pairs | `da3e3ed5-97b1-4d6f-a118-d4f0a6cdb4b4` | 0.00 | 2 |
| build HI-Medium 2022-09-16 pairs | `a7efe362-2e83-41fc-b3b9-a835b3912319` | 0.12 | 295 |
| build HI-Medium 2022-09-22 pairs | `cfa31e8e-ddeb-41ad-9114-a88f53d64195` | 0.00 | 3 |
| build HI-Medium 2022-09-20 triples | `5441abc9-a091-4aa7-975d-60c38c8c0aa9` | 0.00 | 2 |
| build HI-Medium 2022-09-23 pairs | `348ceb2d-1ccd-40b3-9076-3ce40b88e6d0` | 0.00 | 3 |
| build HI-Medium 2022-09-21 triples | `3ed204ee-d8e9-4f8e-90a6-85267a34e3e1` | 0.00 | 2 |
| build HI-Medium 2022-09-22 triples | `874dab31-a0de-4acd-b656-ac849b60807d` | 0.00 | 3 |
| build HI-Medium 2022-09-23 triples | `1edca2eb-3c89-4d77-a3de-c6a34984a62a` | 0.00 | 2 |
| build HI-Medium 2022-09-24 pairs | `8e4826be-d80f-41a8-9595-499c87adc1c1` | 0.00 | 2 |
| build HI-Medium 2022-09-16 triples | `9a763d74-8658-4224-88ec-75b7c99f889b` | 0.21 | 225 |
| build HI-Medium 2022-09-26 pairs | `570fcd42-177b-483b-84af-7b468ec15c2c` | 0.00 | 3 |
| build HI-Medium 2022-09-25 pairs | `d2a81b50-3b3d-405a-87b3-60f505793c52` | 0.00 | 2 |
| build HI-Medium 2022-09-24 triples | `812702a5-9ede-4ece-8aa3-cb10cefeda48` | 0.00 | 2 |
| build HI-Medium 2022-09-27 pairs | `d7e0eb97-d286-48d6-a764-a7fa7704c414` | 0.00 | 2 |
| build HI-Medium 2022-09-26 triples | `72a335a8-7500-40e7-b967-cd295cca7ae2` | 0.00 | 3 |
| build HI-Medium 2022-09-25 triples | `1a47a6b0-5adf-4932-94d6-0203a1cda322` | 0.00 | 2 |
| build HI-Medium 2022-09-27 triples | `005b5fa3-614a-433a-9811-b6d9d085202e` | 0.00 | 2 |
| build LI-Medium 2022-09-01 pairs | `5271b4f0-ad2a-43fe-8af5-9b8d3fd9d660` | 0.23 | 18 |
| build HI-Medium 2022-09-28 pairs | `84ef8b06-f6be-4ca9-9688-fce74e0b327f` | 0.00 | 2 |
| build LI-Medium 2022-09-01 triples | `e9209857-ada5-448d-8ca8-0e66bcf79a90` | 0.46 | 31 |
| build HI-Medium 2022-09-28 triples | `c04401f5-34e8-427e-bd9d-43ca410323f0` | 0.00 | 2 |
| build LI-Medium 2022-09-03 pairs | `8441772b-c4c3-44ab-a547-f4ed59257c54` | 0.05 | 6 |
| build LI-Medium 2022-09-04 pairs | `ad074f66-b0a5-460f-8a1c-fd8cd424561c` | 0.07 | 8 |
| build LI-Medium 2022-09-03 triples | `905fb5a4-4929-4a59-9792-c831b69c4adf` | 0.13 | 6 |
| build LI-Medium 2022-09-04 triples | `2a69d0e2-3958-4f1a-8eed-9279f2b2a3f0` | 0.18 | 8 |
| build LI-Medium 2022-09-06 pairs | `eb86aeb2-b68c-48a3-889e-ac4ffe57be9a` | 0.11 | 12 |
| build LI-Medium 2022-09-05 pairs | `3609e4fd-eb99-4bcd-89b5-af0ad0be92b0` | 0.11 | 12 |
| build LI-Medium 2022-09-06 triples | `23408cf0-4077-46a4-a150-28a83c6a4b8c` | 0.25 | 16 |
| build LI-Medium 2022-09-07 pairs | `0b5417d6-5422-4e8f-8d65-ec7598257293` | 0.11 | 11 |
| build LI-Medium 2022-09-05 triples | `4a39b94e-4464-4680-b210-bc8c0c0c1cc9` | 0.25 | 18 |
| build LI-Medium 2022-09-08 pairs | `dccda2da-6ec0-4590-80e9-14d653083818` | 0.12 | 12 |
| build LI-Medium 2022-09-07 triples | `42062250-a43a-420a-85fb-bd8e72a82afa` | 0.27 | 12 |
| build LI-Medium 2022-09-08 triples | `c47f651b-edc1-4498-aefe-d7b693983f5f` | 0.27 | 18 |
| build LI-Medium 2022-09-02 pairs | `83d0505c-0c0f-4886-9253-bacc501a5287` | 0.13 | 365 |
| build LI-Medium 2022-09-10 pairs | `de319609-11ec-45c7-896c-cfdbe3ef1e19` | 0.05 | 6 |
| build LI-Medium 2022-09-10 triples | `97eb1729-6bb0-424d-a205-7901ca41b638` | 0.13 | 7 |
| build LI-Medium 2022-09-11 pairs | `91d38e64-8688-4be2-9e86-0521146867e6` | 0.07 | 10 |
| build LI-Medium 2022-09-12 pairs | `438ffc50-6b0e-4154-aeee-10cc153284fb` | 0.11 | 11 |
| build LI-Medium 2022-09-11 triples | `df4f1532-9ba9-4dd4-8cc0-da23763a880c` | 0.18 | 8 |
| build LI-Medium 2022-09-12 triples | `83b2de39-21ef-450f-82c6-06291d1d7142` | 0.25 | 16 |
| build LI-Medium 2022-09-13 pairs | `14db8b95-d6fa-4a65-8aac-51d0ac9cd65a` | 0.11 | 11 |
| build LI-Medium 2022-09-09 pairs | `50846b97-30ea-42fe-9eaa-0b1e853ef2a1` | 0.12 | 333 |
| build LI-Medium 2022-09-13 triples | `2f583ed0-a1ed-4393-b56f-e444560355dd` | 0.25 | 15 |
| build LI-Medium 2022-09-14 pairs | `2f364b18-cf77-4e77-b5b1-1783af3012db` | 0.11 | 12 |
| build LI-Medium 2022-09-02 triples | `f0e3c095-3415-44cf-8f8f-71d82181d59a` | 0.25 | 378 |
| build LI-Medium 2022-09-15 pairs | `39b02d1f-7006-4d1e-8e0e-032aac144dd0` | 0.13 | 12 |
| build LI-Medium 2022-09-14 triples | `77b08b1e-909b-4cd9-aadf-6e02c4af8957` | 0.27 | 13 |
| build LI-Medium 2022-09-15 triples | `33dea064-ba55-44a5-ba12-79f4fe30029a` | 0.27 | 18 |
| build LI-Medium 2022-09-17 pairs | `1f82b6fd-fb02-462a-845e-4e306c3d14b7` | 0.00 | 2 |
| build LI-Medium 2022-09-17 triples | `1c2014a6-93b5-4763-b4c2-61d92820d287` | 0.00 | 2 |
| build LI-Medium 2022-09-18 pairs | `76559f5d-91dc-4a0b-9ee3-9eeeb076195d` | 0.00 | 2 |
| build LI-Medium 2022-09-19 pairs | `c48ca34f-ae2d-4e97-92f4-bf051da5a695` | 0.00 | 3 |
| build LI-Medium 2022-09-09 triples | `c7cb53c5-4823-4e55-ab5b-b557a17a219a` | 0.23 | 344 |
| build LI-Medium 2022-09-18 triples | `9b1b86f0-a8cf-4dc7-b6a2-e1af817dda84` | 0.00 | 2 |
| build LI-Medium 2022-09-19 triples | `baf91b25-f7d6-4aa7-bae8-64dfb16f457b` | 0.00 | 2 |
| build LI-Medium 2022-09-16 pairs | `bd8a5931-2922-4b55-9751-fe3ca0acd1ea` | 0.11 | 285 |
| build LI-Medium 2022-09-20 pairs | `fae7f3d4-0d17-4c07-ba0b-ef272fabe276` | 0.00 | 3 |
| build LI-Medium 2022-09-20 triples | `99e88586-d6bf-4488-a4a9-bf0d46d42d8a` | 0.00 | 2 |
| build LI-Medium 2022-09-21 pairs | `ad26929f-0a0a-43a0-8a85-2439bc53b71c` | 0.00 | 2 |
| build LI-Medium 2022-09-23 pairs | `5a09ed28-2010-4ded-abbc-37f854329035` | 0.00 | 2 |
| build LI-Medium 2022-09-22 pairs | `abc3a075-21cd-48f4-b701-026c198efdab` | 0.00 | 2 |
| build LI-Medium 2022-09-21 triples | `0ba7a64a-4343-40a3-9132-7572df822f41` | 0.00 | 2 |
| build LI-Medium 2022-09-22 triples | `ab861d1f-61e7-4a68-953c-aafad27ee396` | 0.00 | 2 |
| build LI-Medium 2022-09-23 triples | `dc4410e4-9f54-4b4a-af89-99c00081d546` | 0.00 | 2 |
| build LI-Medium 2022-09-16 triples | `631e3c0e-85e9-431b-b11a-36b78d7ce5a2` | 0.20 | 210 |
| build LI-Medium 2022-09-24 pairs | `e3a6e28d-a341-41b1-b0dc-3291eaf082c0` | 0.00 | 2 |
| build LI-Medium 2022-09-24 triples | `535724ad-8061-40fb-aafe-ef38930c6952` | 0.00 | 2 |
| build LI-Medium 2022-09-25 pairs | `17ff6922-40c3-46e5-b4a8-4858cd10c883` | 0.00 | 3 |
| build LI-Medium 2022-09-26 pairs | `9735c298-e30e-4a38-b6c4-6ee399da3059` | 0.00 | 2 |
| build LI-Medium 2022-09-27 pairs | `d74336fb-c926-4b3d-b347-cdb2ced0f99c` | 0.00 | 2 |
| build LI-Medium 2022-09-25 triples | `9698d38d-54d4-4184-b391-10734a3b1535` | 0.00 | 2 |
| build LI-Medium 2022-09-26 triples | `576676e2-029b-46ed-97b8-93b217047740` | 0.00 | 2 |
| build LI-Medium 2022-09-27 triples | `9c9e0eb8-cd9e-40c9-994a-f3c6d1ea57c4` | 0.00 | 2 |
| publish bq2_sequences_b | `47c16500-93db-4d6e-8ee2-eed82c3ca4b0` | 0.00 | 1 |
