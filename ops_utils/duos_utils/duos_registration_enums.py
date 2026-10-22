"""
Display values for the enums in a DUOS study registration.

Reading a registration (`GET /api/dataset/study/registration/{id}`) returns enum *names* (e.g. `OBSERVATIONAL`), but
updating one (`PUT /api/dataset/study/{id}`) only accepts the enum *display values* (e.g. `Observational`), so a
registration has to be translated before it is sent back. Enums whose names and display values are identical
(e.g. NIH institutes/centers) are left out. Mirrors the enums in DataBiosphere/consent.
"""

_STUDY_TYPES = {
    'OBSERVATIONAL': 'Observational',
    'INTERVENTIONAL': 'Interventional',
    'DESCRIPTIVE': 'Descriptive',
    'ANALYTICAL': 'Analytical',
    'PROSPECTIVE': 'Prospective',
    'RETROSPECTIVE': 'Retrospective',
    'CASE_REPORT': 'Case report',
    'CASE_SERIES': 'Case series',
    'CROSS_SECTIONAL': 'Cross-sectional',
    'COHORT_STUDY': 'Cohort study',
}
_NIH_ANVIL_USE = {
    'I_AM_NHGRI_FUNDED_AND_I_HAVE_A_DB_GA_P_PHS_ID_ALREADY': 'I am NHGRI funded and I have a dbGaP PHS ID already',
    'I_AM_NHGRI_FUNDED_AND_I_DO_NOT_HAVE_A_DB_GA_P_PHS_ID': 'I am NHGRI funded and I do not have a dbGaP PHS ID',
    'I_AM_NOT_NHGRI_FUNDED_BUT_I_AM_SEEKING_TO_SUBMIT_DATA_TO_AN_VIL': 'I am not NHGRI funded but I am seeking to submit data to AnVIL',  # noqa: E501
    'I_AM_NOT_NHGRI_FUNDED_AND_DO_NOT_PLAN_TO_STORE_DATA_IN_AN_VIL': 'I am not NHGRI funded and do not plan to store data in AnVIL',  # noqa: E501
}
_ADSP_DATA_SUBMITTED = {
    'WITHIN_3_MONTHS_OF_THE_LAST_DATA_GENERATED_OR_LAST_CLINICAL_VISIT': 'Within 3 months of the last data generated or last clinical visit',  # noqa: E501
    'BY_BATCHES_OVER_STUDY_TIMELINE_E_G_BASED_ON_CLINICAL_TRIAL_ENROLLMENT_BENCHMARKS': 'By batches over Study Timeline (e.g. based on clinical trial enrollment benchmarks)',  # noqa: E501
}
_ADSP_ACCESS_MANAGEMENT = {
    'CONTROLLED_ACCESS': 'Controlled Access',
    'OPEN_ACCESS': 'Open Access',
    'EXTERNAL_ACCESS': 'External Access',
}
_ADSP_REASONS = {
    'LEGAL_RESTRICTIONS': 'Legal Restrictions',
    'INFORMED_CONSENT_PROCESSES_ARE_INADEQUATE_TO_SUPPORT_DATA_FOR_SHARING_FOR_THE_FOLLOWING_REASONS': 'Informed consent processes are inadequate to support data for sharing for the following reasons:',  # noqa: E501
    'THE_CONSENT_FORMS_ARE_UNAVAILABLE_OR_NON_EXISTENT_FOR_SAMPLES_COLLECTED_AFTER_JANUARY_25_2015': 'The consent forms are unavailable or non-existent for samples collected after January 25, 2015',  # noqa: E501
    'THE_CONSENT_PROCESS_DID_NOT_SPECIFICALLY_ADDRESS_FUTURE_USE_OR_BROAD_DATA_SHARING_FOR_SAMPLES_COLLECTED_AFTER_JANUARY_25_2015': 'The consent process did not specifically address future use or broad data sharing for samples collected after January 25, 2015',  # noqa: E501
    'THE_CONSENT_PROCESS_INADEQUATELY_ADDRESSES_RISKS_RELATED_TO_FUTURE_USE_OR_BROAD_DATA_SHARING_FOR_SAMPLES_COLLECTED_AFTER_JANUARY_25_2015': 'The consent process inadequately addresses risks related to future use or broad data sharing for samples collected after January 25, 2015',  # noqa: E501
    'THE_CONSENT_PROCESS_SPECIFICALLY_PRECLUDES_FUTURE_USE_OR_BROAD_DATA_SHARING_INCLUDING_A_STATEMENT_THAT_USE_OF_DATA_WILL_BE_LIMITED_TO_THE_ORIGINAL_RESEARCHERS': 'The consent process specifically precludes future use or broad data sharing (including a statement that use of data will be limited to the original researchers)',  # noqa: E501
    'OTHER_INFORMED_CONSENT_LIMITATIONS_OR_CONCERNS': 'Other informed consent limitations or concerns',
    'OTHER': 'Other',
}
_ACCESS_MANAGEMENT = {
    'OPEN': 'open',
    'CONTROLLED': 'controlled',
    'EXTERNAL': 'external',
}
_DATA_LOCATIONS = {
    'AN_VIL_WORKSPACE': 'AnVIL Workspace',
    'TERRA_WORKSPACE': 'Terra Workspace',
    'TDR_LOCATION': 'TDR Location',
    'NOT_DETERMINED': 'Not Determined',
    'OTHER': 'Other',
}
_FILE_TYPES = {
    'ARRAYS': 'Arrays',
    'GENOME': 'Genome',
    'EXOME': 'Exome',
    'SURVEY': 'Survey',
    'PHENOTYPE': 'Phenotype',
}
