
======================================================================
BBOA Media Linkage Demo
Output directory: /tmp/demo_media_linkage_9srysbiv
=======================================================================

======================================================================
Phase 1: URL-Based Retrieval (Workflow → BBAO)
======================================================================

[Workflow Caches Loaded]
  → 1188 EO implementation records
  → 696 media linking records

[Workflow Cache] EO 14183:
  → 181 total communication records
  → 0 filtered (confidence < 0.00)
  → 181 passed confidence filter
  → 181 unique primary URLs
     1. https://appropriations.house.gov/news/press-releases/fy26-defense-priorities-advancing-military-strength-and-readiness
     2. https://chu.house.gov/sites/evo-subsites/chu.house.gov/files/evo-media-document/10.10.2025-letter-to-secretary-hegseth-on-military-hazing-and-bullying-from-rep-judy-chu_0.pdf
     3. https://editorials.voa.gov/a/charting-an-america-defense-policy/8091478.html
     ... and 178 more
| 2026-04-04 11:41:53,984 | INFO - BBAORetriever - Loading BBAO cache: bboa_full_20260310_194954
| 2026-04-04 11:41:54,097 | INFO - BBAORetriever - Loading BBAO cache: bboa_trump_20260311_220355
| 2026-04-04 11:41:54,224 | INFO - BBAORetriever - Loading BBAO cache: bboa_combined_20260326_153800
| 2026-04-04 11:41:54,375 | INFO - BBAORetriever - Loading BBAO cache: bboa_20260327_141107
| 2026-04-04 11:41:54,403 | INFO - BBAORetriever - Loaded 4 BBAO cache(s): 965 eo_events, 616 linked_media, 1335 merged_events, 1335 timeline, 14113 unlinked

[BBAO Cache] Loaded from 4 sources:
  → Sessions: bboa_full_20260310_194954, bboa_trump_20260311_220355, bboa_combined_20260326_153800, bboa_20260327_141107
  → 965 eo_events, 616 linked_media (deduplicated)
  → 14113 unlinked articles, 189 unique EOs

[URL Matching] Tracing 181 URLs through BBAO:
  → Matched 216 events (via source_url)
  → 216 unique event_keys

[EO Grounding Events] 181 events from workflow cache:
  [unrelated] Publication of historical military honors. While thematically related to the 'warrior ethos', this is a routine histo...
    Official: U.S. Department of Defense | Date: 2025-01-03 | Conf: 0.10
    URL: https://valor.defense.gov/Portals/24/Army%20DSC%20List%20-%20Vietnam%20War%20-%202025%2001%2003.pdf
  [announces] Committee policy-vetting document covering readiness, strategic defense priorities, and service-member policy positio...
    Official: Senate Armed Services Committee | Date: 2025-01-06 | Conf: 0.56
    URL: https://www.armed-services.senate.gov/imo/media/doc/hegseth_apq_responses.pdf
  [announces] Pre-confirmation oversight letter probing Hegseth's views on military readiness, management, and combat-role policy.
    Official: Office of Senator Elizabeth Warren | Date: 2025-01-06 | Conf: 0.52
    URL: https://www.warren.senate.gov/imo/media/doc/letter_to_mrpetehegseth.pdf
  [announces] Argued that Pentagon reform should restore lethality, recruiting strength, and combat effectiveness over bureaucracy ...
    Official: U.S. Senator Rick Scott | Date: 2025-01-08 | Conf: 0.65
    URL: https://www.rickscott.senate.gov/2025/1/why-the-pentagon-needs-pete-hegseth
  [announces] Press release advocating confirmation of Hegseth to restore hard power, readiness, merit, and military lethality.
    Official: U.S. Senate / Senator John Barrasso | Date: 2025-01-15 | Conf: 0.68
    URL: https://www.barrasso.senate.gov/newsroom-news-releases-confirm-pete-hegseth-to-restore-american-strength/
  [implements] Official White House issuance of the order setting new service, fitness, and readiness standards and reversing the pr...
    Official: The White House | Date: 2025-01-27 | Conf: 0.99
    URL: https://www.whitehouse.gov/presidential-actions/2025/01/prioritizing-military-excellence-and-readiness/
  [announces] 919th Special Operations Wing republication announced a warfighting-first agenda centered on warrior culture, high st...
    Official: 919th Special Operations Wing | Date: 2025-01-27 | Conf: 0.88
    URL: https://www.919sow.afrc.af.mil/News/Article-Display/Article/4043185/secretary-of-defense-pete-hegseth-begins-most-important-deployment-of-my-life/
  [announces] AETC communication announced a warfighting-first agenda centered on warrior culture, high standards, accountability, ...
    Official: Air Education and Training Command | Date: 2025-01-27 | Conf: 0.89
    URL: https://www.aetc.af.mil/News/Article-Display/Article/4042297/secretary-of-defense-pete-hegseth-begins-most-important-deployment-of-my-life/
  [announces] DOD News announced a warfighting-first program centered on lethality, readiness, accountability, high standards, and ...
    Official: DOD News | Date: 2025-01-27 | Conf: 0.85
    URL: https://www.af.mil/News/Article-Display/Article/4042816/secretary-of-defense-pete-hegseth-begins-most-important-deployment-of-my-life/
  [] Secretary of Defense Pete Hegseth held a town hall meeting at the Pentagon on February 7, 2025, to outline his strate...
    Official: Secretary of Defense | Date: 2025-01-29 | Conf: 0.00
    URL: https://www.war.gov/News/Releases/Release/Article/4055479/readout-of-secretary-of-defense-pete-hegseths-meeting-with-israeli-prime-minist/
  [] On February 7, Secretary of Defense Pete Hegseth held a town hall at the Pentagon to outline his strategic framework ...
    Official: President Trump | Date: 2025-01-29 | Conf: 0.00
    URL: https://www.war.gov/News/Transcripts/Transcript/Article/4064276/defense-secretary-pete-hegseth-holds-media-availability-in-stuttgart-germany/
  [announces] Official editorial articulated support for a lethal, merit-based fighting force and framed readiness and combat effec...
    Official: VOA Editorials / U.S. Agency for Global Media | Date: 2025-01-31 | Conf: 0.78
    URL: https://editorials.voa.gov/a/in-support-of-a-lethal-and-meritorious-fighting-force/7958540.html
  [extends] Defense secretary announced AFRICOM strikes against ISIS-Somalia operatives, underscoring the restored deterrence and...
    Official: U.S. Department of War | Date: 2025-02-01 | Conf: 0.55
    URL: https://www.war.gov/News/Releases/Release/Article/4050461/secretary-of-defense-pete-hegseth-statement-on-us-africa-command-strikes-in-som/
  [announces] Announced allied engagements focused on deterrence, warfighter support, NATO burden-sharing, and defense readiness.
    Official: Secretary of Defense Pete Hegseth / U.S. Mission Poland | Date: 2025-02-07 | Conf: 0.55
    URL: https://pl.usembassy.gov/secretary-of-defense-pete-hegseth-to-travel-to-germany-belgium-and-poland/
  [announces] Town hall reiterated priorities to revive the warrior ethos, rebuild the department, and reestablish deterrence throu...
    Official: U.S. Department of War | Date: 2025-02-07 | Conf: 0.91
    URL: https://www.war.gov/News/News-Stories/Article/Article/4060150/defense-secretary-underscores-dod-priorities-during-pentagon-town-hall/
  [announces] Outlined the department's readiness agenda centered on restoring the warrior ethos, rebuilding the military, and rees...
    Official: Secretary of Defense Pete Hegseth / U.S. Department of War | Date: 2025-02-07 | Conf: 0.89
    URL: https://www.war.gov/News/Transcripts/Transcript/Article/4060651/secretary-of-defense-pete-hegseth-town-hall-at-the-pentagon/
  [announces] Told NATO defense ministers that deterrence depends on greater hard-power investment, readiness, and defense-industri...
    Official: Secretary of Defense Pete Hegseth / U.S. Department of War | Date: 2025-02-13 | Conf: 0.61
    URL: https://www.war.gov/News/News-Stories/Article/Article/4066810/hegseth-tells-nato-hard-power-provides-deterrence-defense/
  [implements] Inspected Army Prepositioned Stocks-2 equipment and highlighted the Transforming in Contact initiative testing new ca...
    Official: U.S. Army V Corps | Date: 2025-02-15 | Conf: 0.89
    URL: https://www.vcorps.army.mil/Newsroom/Articles/Article-Display/Article/4069575/us-defense-secretary-hegseth-inspects-military-equipment-in-poland/
  [announces] Announced senior-officer nominations framed around selecting leaders who embody the warfighter ethos and the mission ...
    Official: U.S. Department of War | Date: 2025-02-21 | Conf: 0.87
    URL: https://www.war.gov/News/Releases/Release/Article/4074482/secretary-of-defense-pete-hegseth-statement-on-general-officer-nominations/
  [announces] House oversight Democrats argued that abrupt firings of senior military leaders threatened readiness, morale, and nat...
    Official: U.S. House Committee on Oversight and Government Reform (Democratic Staff) | Date: 2025-02-24 | Conf: 0.64
    URL: https://oversightdemocrats.house.gov/imo/media/doc/2025-02-24.GEC%20and%20MFA%20to%20Hegseth%20re%20DOD%20Firings%20FINAL.pdf
  [announces] President Trump nominated Lt. Gen. Dan Caine to chair the Joint Chiefs, explicitly framing the choice around deterrin...
    Official: President Donald J. Trump / U.S. Department of War | Date: 2025-02-26 | Conf: 0.86
    URL: https://www.war.gov/News/News-Stories/Article/Article/4077712/trump-nominates-dan-razin-caine-to-be-joint-chiefs-chairman/
  [implements] Issued the medical-standards and service-policy guidance treating gender dysphoria as incompatible with service absen...
    Official: U.S. Department of War / Office of the Secretary of Defense | Date: 2025-02-28 | Conf: 0.98
    URL: https://www.war.gov/News/News-Stories/Article/article/4086111/dod-gender-dysphoria-incompatible-with-military-service-members-must-serve-in-a/
  [implements] DOD: Gender Dysphoria Incompatible With Military, Service Members Must Serve in Accordance With Sex
    Official: U.S. Department of War | Date: 2025-02-28 | Conf: 0.95
    URL: https://www.dinfos.dma.mil/About/DINFOS-Hall-of-Fame/Hall-of-Fame-2023/videoid/954437/dvpTag/unit/
  [extends] Secretary Hegseth restored the Fort Benning name to emphasize installation history, heroism, and the warfighter ethos...
    Official: Secretary of Defense Pete Hegseth / U.S. Department of War | Date: 2025-03-03 | Conf: 0.63
    URL: https://www.war.gov/News/Releases/Release/Article/4090073/secretary-of-defense-pete-hegseth-renames-fort-moore-to-fort-benning/
  [extends] Highlighted merit, discipline, and service as exemplars of the restored warrior ethos and readiness culture.
    Official: U.S. Department of War | Date: 2025-03-05 | Conf: 0.66
    URL: https://www.war.gov/News/Feature-Stories/Story/Article/4108194/a-dream-realized-jason-hartleys-journey-to-west-point/
  [extends] Linked improved military recruiting to the administration's military reset and broader warfighting-readiness agenda.
    Official: President Donald J. Trump / U.S. Department of War | Date: 2025-03-05 | Conf: 0.78
    URL: https://www.war.gov/News/News-Stories/Article/article/4101236/president-says-military-recruiting-up-so-too-will-be-american-shipbuilding-gold/
  [implements] Discontinued most OUSD(R&E) social-science research and redirected resources toward hypersonics, AI-powered systems, ...
    Official: U.S. Department of War / Office of the Secretary of Defense for Research and Engineering | Date: 2025-03-07 | Conf: 0.83
    URL: https://www.war.gov/News/Releases/Release/Article/4113076/pentagon-culls-social-science-research-prioritizes-fiscal-responsibility-and-te/
  [implements] Secretary Hegseth ordered a department-wide review of fitness, body-composition, and grooming standards to restore ri...
    Official: Secretary of Defense Pete Hegseth / U.S. Department of War | Date: 2025-03-12 | Conf: 0.96
    URL: https://www.war.gov/News/Releases/Release/Article/4118161/rapid-force-wide-review-of-military-standards/
  [implements] Defense Department announced the sharpening of service standards around physical fitness, discipline, and warfighting...
    Official: U.S. Department of War / Chief Pentagon Spokesman Sean Parnell | Date: 2025-03-14 | Conf: 0.98
    URL: https://www.war.gov/News/News-Stories/Article/Article/4121054/this-week-defense-department-sharpens-standards-flushes-climate-change-policies/
  [] Defense Secretary Pete Hegseth signs a memorandum aboard an aircraft over the Pacific Ocean.
    Official: Unknown | Date: 2025-03-26 | Conf: 0.00
    URL: https://www.war.gov/Multimedia/Photos/igphoto/2003677644/
  [announces] A DoD article reported nominees' commitments to improve warfighting tools, medical readiness, and operational capabil...
    Official: U.S. Department of War | Date: 2025-03-27 | Conf: 0.61
    URL: https://www.war.gov/News/News-Stories/Article/Article/4137660/defense-department-nominees-promise-better-warfighting-tools-improved-medical-c/
  [extends] Used wartime remembrance and alliance solidarity to reinforce a warrior ethos, sacrifice, and duty-centered military ...
    Official: Secretary of Defense Pete Hegseth / U.S. Department of War | Date: 2025-03-29 | Conf: 0.66
    URL: https://www.war.gov/Multimedia/Photos/igphoto/2003678047/
  [announces] Presented Lt. Gen. Dan Caine's deterrence-first, readiness-focused leadership vision stressing alliances, modernizati...
    Official: U.S. Department of War | Date: 2025-04-01 | Conf: 0.83
    URL: https://www.war.gov/News/News-Stories/Article/Article/4142512/nominee-for-chairman-militarys-top-job-is-creating-peace-through-strength/
  [announces] Recorded Caine's emphasis on deterrence, apolitical military advice, modernization urgency, and reform of defense rea...
    Official: Senate Armed Services Committee | Date: 2025-04-01 | Conf: 0.72
    URL: https://www.armed-services.senate.gov/imo/media/doc/caine_apq_responses.pdf
  [implements] Publicly articulated the program of restoring the warrior ethos, rebuilding the military, and reestablishing deterren...
    Official: Secretary of Defense Pete Hegseth / U.S. Department of War | Date: 2025-04-02 | Conf: 0.92
    URL: https://www.war.gov/News/News-Stories/Article/Article/4143993/hegseth-speaks-to-troops-nfl-players-about-service-and-warrior-ethos/
  [implements] Announced common, uncompromising standards for entry into combat-arms career fields across the services.
    Official: U.S. Department of War | Date: 2025-04-04 | Conf: 0.91
    URL: https://www.war.gov/News/News-Stories/Article/Article/4146717/this-week-in-dod-common-sense-combat-arms-standards-no-halt-on-hammering-houthi/
  [extends] At the Naval Academy, urged midshipmen to build a focused, lethal force grounded in mission, expertise, deterrence, a...
    Official: Secretary of Defense Pete Hegseth / U.S. Department of War | Date: 2025-04-04 | Conf: 0.84
    URL: https://www.war.gov/News/Feature-Stories/Story/Article/4146116/hegseth-keeps-it-candid-with-midshipmen-during-academy-visit/
  [extends] Linked stronger force posture, engineering support, and infrastructure readiness in Japan to deterrence and operation...
    Official: U.S. Army Corps of Engineers - Japan Engineer District | Date: 2025-04-07 | Conf: 0.61
    URL: https://www.poj.usace.army.mil/Media/News-Stories/Article/4148505/secretary-hegseth-were-ready-to-build/
  [unknown] Public and congressional reaction to the implementation of rigorous fitness-for-duty standards and personnel shifts
    Official: Senator Tammy Duckworth / Fox News | Date: 2025-04-13 | Conf: 0.85
    URL: https://www.war.gov/News/News-Stories/Article/Article/4150566/dod-welcomes-back-service-members-impacted-by-covid-19-vaccine-mandate/
  [implements] The Department summarized the first 100 days as a warfighting-first reset centered on lethality, meritocracy, account...
    Official: Secretary of Defense Pete Hegseth / U.S. Department of War | Date: 2025-04-23 | Conf: 0.93
    URL: https://www.war.gov/News/News-Stories/Article/Article/4164270/hegseth-recaps-100-days-of-dod-accomplishments-during-speech-at-war-college/
  [implements] Secretary Hegseth described a return to lethality, high standards, physical readiness, accountability, and a warrior ...
    Official: Secretary of Defense Pete Hegseth / U.S. Department of War | Date: 2025-04-23 | Conf: 0.95
    URL: https://www.war.gov/News/Speeches/Speech/Article/4164715/remarks-by-secretary-of-defense-pete-hegseth-at-the-army-war-college-as-deliver/
  [extends] Public statement applauding bold military reform, the restoration of the warrior ethos, and the prioritization of let...
    Official: Congressman Pat Harrigan | Date: 2025-05-01 | Conf: 0.88
    URL: https://www.war.gov/News/News-Stories/Article/Article/4179206/pentagon-marks-public-service-recognition-week-honors-civilian-workforce/
  [implements] Highlights booming recruiting and readiness metrics following the implementation of new military excellence and biolo...
    Official: U.S. Department of War | Date: 2025-05-02 | Conf: 0.90
    URL: https://www.war.gov/News/News-Stories/Article/Article/4172996/14-winning-weeks-at-dod-border-security-hitting-back-at-houthis-booming-recruit/
  [extends] Secretary of Defense Pete Hegseth Delivers Keynote Address at Special Operations Forces Week 2025
    Official: U.S. Department of War | Date: 2025-05-06 | Conf: 0.85
    URL: https://www.war.gov/News/Transcripts/Transcript/Article/4176603/secretary-of-defense-pete-hegseth-delivers-keynote-address-at-special-operation/
  [extends] Army Armor School codified branch ethos, heritage, and leader-development guidance reinforcing warrior identity and c...
    Official: U.S. Army Armor School, Fort Benning, Georgia | Date: 2025-05-06 | Conf: 0.78
    URL: https://www.benning.army.mil/armor/ocoa/content/References%20and%20Guides/20250506%20USAARMS%20Pam%20360-2%20This%20is%20Armor.pdf
  [extends] Service Leaders Discuss Budget, Combat Readiness
    Official: U.S. Department of War | Date: 2025-05-07 | Conf: 0.80
    URL: https://www.war.gov/News/News-Stories/Article/Article/4178142/service-leaders-discuss-budget-combat-readiness/
  [announces] Issued a presidential proclamation honoring World War II victory and renewing a commitment to vigilance, liberty, and...
    Official: The White House | Date: 2025-05-08 | Conf: 0.63
    URL: https://www.whitehouse.gov/presidential-actions/2025/05/vcitory-day-for-world-war-ii-2025/
  [extends] Special operations leadership set future-force priorities around readiness, multidomain operations, personnel perform...
    Official: Colby Jenkins / U.S. Department of War | Date: 2025-05-08 | Conf: 0.84
    URL: https://www.war.gov/News/News-Stories/Article/Article/4179258/senior-official-outlines-future-priorities-for-special-ops/
  [implements] Department of War reviewed service-member standards, reduced top-heavy leadership, and emphasized restoration of a wa...
    Official: U.S. Department of War | Date: 2025-05-09 | Conf: 0.92
    URL: https://www.war.gov/News/News-Stories/Article/Article/4180453/this-week-in-dod-refocusing-resources-service-member-standards-red-sea-ceasefire/
  [implements] Transcript of a Senior Defense Official Background Briefing on Prioritizing Military Excellence and Readiness: Implem...
    Official: U.S. Department of War | Date: 2025-05-15 | Conf: 0.98
    URL: https://www.jbsa.mil/News/News/Article/4118739/senior-joint-force-leaders-testify-on-border-operations-missile-defense/
  [implements] Detailed accession halt, voluntary separation timelines, and involuntary separation procedures for service members wi...
    Official: Senior Defense Official / U.S. Department of War | Date: 2025-05-15 | Conf: 0.98
    URL: https://www.war.gov/News/Transcripts/Transcript/Article/4188052/transcript-of-a-senior-defense-official-background-briefing-on-prioritizing-mil/
  [implements] Published formal department guidance halting accessions and setting separation procedures for service members with ge...
    Official: U.S. Department of War | Date: 2025-05-16 | Conf: 0.99
    URL: https://www.war.gov/News/News-Stories/Article/Article/4188279/dod-issues-implementation-guidance-on-separation-of-service-members-with-gender/
  [implements] DOD Issues Implementation Guidance on Separation of Service Members With Gender Dysphoria
    Official: U.S. Department of War | Date: 2025-05-16 | Conf: 0.98
    URL: https://www.war.gov/News/Tag/298368/feinberg/
  [unknown] Secretary Hegseth Statement Directing DOD Review of U.S. Military Withdrawal From Afghanistan to ensure accountabilit...
    Official: U.S. Department of War | Date: 2025-05-20 | Conf: 0.80
    URL: https://www.war.gov/News/Releases/Release/Article/4193103/secretary-hegseth-statement-directing-dod-review-of-us-military-withdrawal-from/
  [extends] Hegseth told the 82nd Airborne that the administration would restore the warrior ethos through training, readiness, a...
    Official: Secretary of Defense Pete Hegseth / U.S. Department of War | Date: 2025-05-22 | Conf: 0.94
    URL: https://www.war.gov/News/Transcripts/Transcript/Article/4195999/sd-hegseth-speech-at-82nd-airborne-ft-bragg-nc/
  [extends] Presented the National Guard as a combat-ready, cost-effective force and outlined readiness, modernization, and warfi...
    Official: Air Force Gen. Steven Nordhaus / U.S. Department of War | Date: 2025-05-23 | Conf: 0.67
    URL: https://www.war.gov/News/News-Stories/Article/Article/4196945/national-guard-bureau-chief-says-guard-delivers-incredible-value/
  [extends] Used the West Point commencement address to reinforce warrior ethos, future-war preparedness, and military excellence.
    Official: President Donald J. Trump / U.S. Department of War | Date: 2025-05-24 | Conf: 0.82
    URL: https://www.war.gov/News/News-Stories/Article/Article/4197092/trump-lauds-cadets-for-choosing-service-honor-over-civilian-life/
  [unknown] President Trump Remarks at West Point emphasizing the restoration of the warrior ethos and military excellence.
    Official: U.S. Senate Democrats Newsroom (Transcript of President Trump) | Date: 2025-05-24 | Conf: 0.85
    URL: https://www.democrats.senate.gov/newsroom/trump-transcripts/transcript-president-trump-remarks-at-west-point-52425
  [extends] Defense Secretary's public address reinforced sacrifice, service, and a combat-focused warrior ethos as core military...
    Official: Secretary Pete Hegseth / U.S. Department of War | Date: 2025-05-25 | Conf: 0.63
    URL: https://www.war.gov/News/News-Stories/Article/Article/4197165/hegseth-reminds-americans-freedom-comes-at-a-cost/
  [extends] Senior national leaders commemorated fallen service members and Gold Star families, reinforcing duty, sacrifice, and ...
    Official: President Donald J. Trump; Vice President JD Vance; Secretary Pete Hegseth; Gen. Dan Caine | Date: 2025-05-26 | Conf: 0.61
    URL: https://www.war.gov/News/News-Stories/Article/Article/4197195/trump-vance-hegseth-caine-honor-fallen-gold-star-families/
  [implements] Legislative support and budget alignment for advancing military strength, readiness, and lethality.
    Official: House Committee on Appropriations - Republicans | Date: 2025-06-10 | Conf: 0.95
    URL: https://appropriations.house.gov/news/press-releases/fy26-defense-priorities-advancing-military-strength-and-readiness
  [implements] Testimony aligning military budget requests with the administration's readiness and warfighting-first mandates.
    Official: United States Senate Committee on Appropriations | Date: 2025-06-10 | Conf: 0.88
    URL: https://www.appropriations.senate.gov/download/secretary-peter-b-hegseth-testimony
  [extends] Explained that the FY2026 defense budget would fund the training, standards, and support needed for increasingly task...
    Official: Secretary of Defense Pete Hegseth / U.S. Department of War | Date: 2025-06-12 | Conf: 0.78
    URL: https://www.war.gov/News/News-Stories/Article/Article/4215588/defense-secretary-says-funding-request-addresses-adequate-support-for-special-o/
  [unknown] Establishment of a Military Service Recruitment Task Force to reinforce high standards for service.
    Official: U.S. Department of War | Date: 2025-06-18 | Conf: 0.95
    URL: https://www.war.gov/News/Releases/Release/Article/4047141/secretary-of-defense-pete-hegseths-guidance-on-restoring-americas-fighting-force/
  [implements] Announced establishment of a Military Service Recruitment Task Force to remove recruiting obstacles and build on recr...
    Official: Chief Pentagon Spokesman Sean Parnell / U.S. Department of War | Date: 2025-06-18 | Conf: 0.90
    URL: https://www.war.gov/News/Releases/Release/Article/4221383/statement-by-chief-pentagon-spokesman-sean-parnell-on-the-memorandum-for-the-es/
  [] On June 18, 2025, Secretary of Defense Pete Hegseth directed the establishment of a Military Service Recruitment Task...
    Official: President Donald | Date: 2025-06-18 | Conf: 0.00
    URL: https://www.war.gov/Multimedia/Photos/igphoto/2003744912/
  [implements] Department of War created a Military Recruitment Task Force to capitalize on recruiting gains and remove onboarding b...
    Official: U.S. Department of War | Date: 2025-06-20 | Conf: 0.90
    URL: https://www.war.gov/News/News-Stories/Article/Article/4222328/this-week-in-dod-new-recruiting-task-force-greenland-shifts-to-northcom-dod-act/
  [announces] Air Force republication presented Operation Midnight Hammer as a successful precision strike demonstrating readiness,...
    Official: U.S. Air Force | Date: 2025-06-22 | Conf: 0.58
    URL: https://www.af.mil/News/Article-Display/Article/4222562/hegseth-caine-laud-success-of-us-strike-on-iran-nuke-sites/
  [announces] Senior defense leaders announced and defended precision strikes on Fordow, Natanz, and Isfahan as a decisive warfight...
    Official: Secretary of Defense Pete Hegseth and Chairman of the Joint Chiefs of Staff Gen. Dan Caine / U.S. Department of War | Date: 2025-06-22 | Conf: 0.66
    URL: https://www.war.gov/News/Transcripts/Transcript/Article/4222543/secretary-of-defense-pete-hegseth-and-chairman-of-the-joint-chiefs-of-staff-gen/
  [announces] STRATCOM article emphasized the strike's success, operational surprise, and joint-force capability.
    Official: U.S. Strategic Command | Date: 2025-06-22 | Conf: 0.58
    URL: https://www.stratcom.mil/Media/News/News-Article-View/Article/4223163/hegseth-caine-laud-success-of-us-strike-on-iran-nuke-sites/
  [announces] Announced Secretary Hegseth's NATO Summit travel to press allies on higher defense spending and stronger European con...
    Official: Chief Pentagon Spokesman Sean Parnell / U.S. Department of War | Date: 2025-06-24 | Conf: 0.62
    URL: https://www.war.gov/News/Advisories/Advisory/Article/4223935/secretary-of-defense-pete-hegseth-to-travel-to-the-hague-netherlands/
  [extends] Department feature highlighted the 15-year planning and specialized munitions development behind the Fordow strike.
    Official: U.S. Department of War | Date: 2025-06-26 | Conf: 0.61
    URL: https://www.war.gov/News/News-Stories/Article/Article/4227082/historically-successful-strike-on-iranian-nuclear-site-was-15-years-in-the-maki/
  [extends] National Guard article highlighted Missouri Guard Airmen's operational role in Operation Midnight Hammer, underscorin...
    Official: National Guard | Date: 2025-06-27 | Conf: 0.69
    URL: https://www.nationalguard.mil/news/article-view/article/4228050/missouri-guard-airmen-play-critical-role-in-iran-nuclear-strike-mission/
  [implements] Army Armor School codified branch identity, mission, and leader-development guidance centered on combat effectiveness...
    Official: U.S. Army Armor School, Fort Benning, Georgia | Date: 2025-07-01 | Conf: 0.86
    URL: https://www.benning.army.mil/Armor/OCoA/content/References%20and%20Guides/20250702%20USAARMS%20Pam%20360-2%20This%20is%20Armor.pdf
  [extends] USEUCOM marked a command transition while emphasizing deterrence, integrated operations, and operational readiness ac...
    Official: United States European Command | Date: 2025-07-01 | Conf: 0.66
    URL: https://www.eucom.mil/pressrelease/43172/useucom-welcomes-new-commander-in-stuttgart-ceremony
  [unknown] Recruitment Task Force seeks to capitalize on enlistment surge, aligning with the restoration of merit-based standards.
    Official: The United States Army | Date: 2025-07-02 | Conf: 0.90
    URL: https://www.war.gov/News/Releases/Release/Article/4040940/secretary-hegseths-message-to-the-force/
  [implements] Pentagon Provides Update on Southern Border, Recruitment Numbers
    Official: U.S. Department of War | Date: 2025-07-02 | Conf: 0.95
    URL: https://www.war.gov/News/News-Stories/Article/Article/4234168/pentagon-provides-update-on-southern-border-recruitment-numbers/
  [implements] Army detailed the Recruitment Task Force created to sustain enlistment gains while preserving force quality and readi...
    Official: The United States Army | Date: 2025-07-02 | Conf: 0.88
    URL: https://www.army.mil/article/286799/recruitment_task_force_seeks_to_capitalize_on_2025_enlistment_surge
  [extends] The Pentagon briefing reported strong recruiting performance and framed force-management decisions and global commitm...
    Official: Chief Pentagon Spokesman Sean Parnell / U.S. Department of War | Date: 2025-07-02 | Conf: 0.90
    URL: https://www.war.gov/News/Transcripts/Transcript/Article/4234712/chief-pentagon-spokesman-sean-parnell-holds-press-briefing/
  [extends] Secretary Hegseth publicly articulated a warrior-ethos framework centered on courage, mission focus, recruiting reviv...
    Official: Secretary of Defense Pete Hegseth / The White House | Date: 2025-07-06 | Conf: 0.93
    URL: https://www.whitehouse.gov/videos/reviving-the-warrior-spirit-secretary-of-defense-pete-hegseth-on-service-sacrifice-strength/
  [unknown] Secretary of Defense Pete Hegseth remarks on reviving the warrior spirit, service, sacrifice, and strength.
    Official: The White House | Date: 2025-07-06 | Conf: 0.85
    URL: https://www.war.gov/About/Biographies/Biography/article/4040890/hon-pete-hegseth/
  [unknown] Hosting NFL Rookie Rayuan Lane at the Pentagon to promote military service, physical fitness, and recruitment.
    Official: U.S. Department of War | Date: 2025-07-07 | Conf: 0.70
    URL: https://www.war.gov/Multimedia/Photos/igphoto/2003748009/
  [extends] Army leadership reaffirmed that trained ground forces, interoperability, and combat-ready formations remain indispens...
    Official: The United States Army | Date: 2025-07-16 | Conf: 0.74
    URL: https://www.army.mil/article/287107/ground_forces_remain_critical_amid_service_transformation_says_army_leader
  [implements] The 25th Infantry Division replaced legacy howitzers with HIMARS and retrained forces to increase Pacific lethality a...
    Official: U.S. Department of War / 25th Infantry Division | Date: 2025-07-17 | Conf: 0.95
    URL: https://www.war.gov/News/News-Stories/Article/Article/4247291/armys-tropic-lightning-division-enhances-lethality-in-pacific-with-himars-retra/
  [extends] Defense Secretary Reminds Young Leaders They Will One Day Lead Nation (Speech reinforcing the warrior ethos and high ...
    Official: U.S. Department of War | Date: 2025-07-23 | Conf: 0.95
    URL: https://www.war.gov/News/News-Stories/Article/Article/4253807/defense-secretary-reminds-young-leaders-they-will-one-day-lead-nation/
  [extends] Defense leaders used Purple Heart Day to reinforce sacrifice, courage, and the warrior ethos as core military values ...
    Official: The United States Army / DOD leaders Pete Hegseth and Sean Parnell | Date: 2025-08-07 | Conf: 0.74
    URL: https://www.army.mil/article/287649/purple_heart_represents_best_americans_say_dod_leaders
  [extends] Department recap tied tightened standards, recruitment gains, drone dominance, and servicemember transition programs ...
    Official: U.S. Department of War | Date: 2025-08-08 | Conf: 0.71
    URL: https://www.war.gov/News/News-Stories/Article/Article/4268806/this-week-in-dod-200-days-of-impact-fort-bliss-assumes-new-role-in-homeland-def/
  [unknown] Graduation ceremony reflecting the continuous enforcement of rigorous military training and readiness standards.
    Official: Officer Candidates School, Training Command, Quantico, Virginia | Date: 2025-08-09 | Conf: 0.75
    URL: https://www.ocs.marines.mil/Portals/243/Docs/Programs/PLC%20Sr%20Program%20PDF.pdf?ver=BlC6SFf5F5GwJIlmbbAdPg%3D%3D
  [unknown] Presidential message commemorating the 80th Anniversary of winning World War II, reinforcing the warrior ethos.
    Official: The White House | Date: 2025-08-14 | Conf: 0.70
    URL: https://www.whitehouse.gov/briefings-statements/2025/08/presidential-message-on-the-80th-anniversary-of-winning-world-war-ii/
  [extends] Leadership-led Pentagon fitness event promoted physical readiness standards for service members and DOD civilians.
    Official: Secretary Pete Hegseth; Secretary Robert F. Kennedy Jr. / U.S. Department of War | Date: 2025-08-19 | Conf: 0.86
    URL: https://www.war.gov/News/News-Stories/Article/Article/4274447/hegseth-rfk-jr-join-service-members-dod-civilians-for-physical-fitness-challenge/
  [extends] The Mexican Border Defense Medal linked recognized service to homeland-defense readiness and a warfighter-centered mi...
    Official: Secretary Pete Hegseth / U.S. Department of War | Date: 2025-08-23 | Conf: 0.74
    URL: https://www.war.gov/News/News-Stories/Article/Article/4283789/hegseth-announces-establishment-of-mexican-border-defense-medal/
  [extends] General Dan Caine told the NGAUS conference that Guard mobilization is integral to deterrence, readiness, and the abi...
    Official: General Dan Caine / Joint Chiefs of Staff | Date: 2025-08-23 | Conf: 0.84
    URL: https://www.jcs.mil/Media/Speeches-and-Transcripts/Transcript-Display/Article/4289295/ngaus-147th-general-conference-and-exhibition/
  [extends] General Caine praised the National Guard's operational role and emphasized readiness, proper arming, global integrati...
    Official: General Dan Caine / 131st Bomb Wing | Date: 2025-08-24 | Conf: 0.85
    URL: https://www.131bw.ang.af.mil/News/Article/4284701/caine-when-you-mobilize-the-guard-you-mobilize-the-nation/
  [extends] Army coverage of Caine's remarks reinforced Guard readiness, deterrence, and warfighting support as core military pri...
    Official: General Dan Caine / The United States Army | Date: 2025-08-25 | Conf: 0.85
    URL: https://www.army.mil/article/288062/caine_thanks_national_guard_for_support_to_nation
  [extends] Army republication connected the new border-service medal to readiness, discipline, and the military's homeland-defen...
    Official: The United States Army | Date: 2025-08-25 | Conf: 0.73
    URL: https://www.army.mil/article/288060/hegseth_announces_establishment_of_mexican_border_defense_medal
  [extends] War Department coverage of Caine's NGAUS remarks emphasized Guard readiness, force integration, deterrence, and the p...
    Official: General Dan Caine / U.S. Department of War | Date: 2025-08-26 | Conf: 0.84
    URL: https://www.war.gov/News/News-Stories/Article/Article/4286123/caine-thanks-national-guard-for-support-to-nation/
  [extends] Marine Week Boston showcased Marine culture, recruiting, and public engagement to reinforce service identity and supp...
    Official: United States Marine Corps | Date: 2025-08-29 | Conf: 0.66
    URL: https://www.marines.mil/News/News-Display/Article/4288818/shipping-up-to-boston-marine-week-boston-2025/
  [extends] Used the Harlem Hellfighters' combat record to reinforce a warrior ethos centered on courage, endurance, sacrifice, a...
    Official: Secretary of Defense Pete Hegseth / U.S. Department of War | Date: 2025-09-03 | Conf: 0.88
    URL: https://www.war.gov/News/News-Stories/Article/Article/4293099/hegseth-recounts-tenacity-valor-of-world-war-i-era-black-infantry-regiment/
  [implements] Announced remarks at Army Officer Candidate School graduation and visits with Airborne School trainees and the 75th R...
    Official: Secretary of Defense Pete Hegseth / U.S. Department of War | Date: 2025-09-03 | Conf: 0.92
    URL: https://www.war.gov/News/Releases/Release/Article/4292465/secretary-of-defense-pete-hegseth-to-travel-to-fort-benning/
  [extends] Secretary Hegseth told new Army officers that endurance, character, and hope are core leadership requirements, reinfo...
    Official: Secretary of War Pete Hegseth / U.S. Department of War | Date: 2025-09-04 | Conf: 0.89
    URL: https://www.war.gov/News/News-Stories/Article/Article/4293828/lead-with-endurance-character-hegseth-tells-new-army-officers/
  [implements] Recruit Graduation Ceremony emphasizing rigorous physical and mental fitness standards for incoming service members.
    Official: United States Coast Guard | Date: 2025-09-05 | Conf: 0.75
    URL: https://www.forcecom.uscg.mil/Portals/3/Documents/TCCM/Documents/Graduation/250905-N207.pdf
  [extends] Chairman of the Joint Chiefs and the SEAC visited USS Fort Lauderdale to engage deployed personnel and assess operati...
    Official: General Dan Caine and Senior Enlisted Advisor David Isom / Joint Chiefs of Staff | Date: 2025-09-08 | Conf: 0.63
    URL: https://www.jcs.mil/
  [extends] Honored Best Ranger Competition winners as exemplars of the restored warrior ethos, elite combat standards, and readi...
    Official: Secretary of War Pete Hegseth / U.S. Department of War | Date: 2025-09-17 | Conf: 0.91
    URL: https://www.war.gov/News/News-Stories/Article/Article/4305953/hegseth-welcomes-top-army-rangers-to-pentagon/
  [] During a speech on September 30, 2025, Secretary of War Pete Hegseth announced new directives to reform military cult...
    Official: Secretary of War | Date: 2025-09-17 | Conf: 0.00
    URL: https://www.war.gov/News/News-Stories/Article/Article/4318394/hegseth-announces-war-department-reforms-in-sweeping-speech-to-top-military-bra/
  [unknown] At War Department, Shaving Waivers Out, Clean-Shaven Faces In
    Official: U.S. Department of War | Date: 2025-09-19 | Conf: 0.95
    URL: https://www.war.gov/News/Releases/Release/Article/4062245/secretary-of-defense-pete-hegseth-renames-fort-liberty-to-fort-bragg/
  [implements] Directed enforcement of stricter grooming standards and phased out long-term shaving waivers to reinforce uniformity,...
    Official: Secretary of War Pete Hegseth / U.S. Department of War | Date: 2025-09-19 | Conf: 0.97
    URL: https://www.war.gov/News/News-Stories/Article/Article/4309929/at-war-department-shaving-waivers-out-clean-shaven-faces-in/
  [extends] President Trump told senior military leaders the force would protect the nation through merit, ability, character, an...
    Official: U.S. Department of War / President Donald J. Trump | Date: 2025-09-30 | Conf: 0.93
    URL: https://www.war.gov/News/News-Stories/Article/Article/4318894/trump-military-will-protect-nation-with-focus-on-merit-reawakened-warrior-spirit/
  [announces] President Trump publicly reinforced a warfighting-first military vision emphasizing high standards, combat effectiven...
    Official: President Donald Trump / The White House | Date: 2025-09-30 | Conf: 0.88
    URL: https://www.whitehouse.gov/gallery/president-donald-trump-makes-remarks-at-a-gathering-of-top-military-leadership/
  [extends] Announced sweeping military reforms centered on warfighting, physical standards, restored discipline, and the warrior...
    Official: Secretary of War Pete Hegseth / U.S. Department of War | Date: 2025-09-30 | Conf: 0.95
    URL: https://www.war.gov/News/Speeches/Tag/296734/hegseth/
  [unknown] CONGRESSMAN PAT RYAN LEADS 25 HOUSE DEMOCRATS IN REJECTING TRUMP’S MESSAGE TO FLAG OFFICERS...
    Official: Congressman Pat Ryan | Date: 2025-10-01 | Conf: 0.85
    URL: https://patryan.house.gov/media/press-releases/congressman-pat-ryan-leads-25-house-democrats-rejecting-trumps-message-flag
  [] On October 10, 2025, Representative Judy Chu sends a letter to Secretary of Defense Pete Hegseth expressing concern o...
    Official: Unknown | Date: 2025-10-01 | Conf: 0.00
    URL: https://chu.house.gov/sites/evo-subsites/chu.house.gov/files/evo-media-document/10.10.2025-letter-to-secretary-hegseth-on-military-hazing-and-bullying-from-rep-judy-chu_0.pdf
  [unknown] Opinion and Order: State of Oregon and the City of Portland v. Donald Trump, et al.
    Official: U.S. District Court for the District of Oregon | Date: 2025-10-04 | Conf: 0.80
    URL: https://www.portland.gov/federal/documents/10-4-2025-state-city-v-trump-temporary-restraining-order-granted/download
  [unknown] Duckworth, Durbin Lead Illinois Democrats in Demanding President Trump Immediately Rescind His Order to Deploy Nation...
    Official: U.S. Senator Tammy Duckworth of Illinois | Date: 2025-10-06 | Conf: 0.85
    URL: https://www.duckworth.senate.gov/news/press-releases/duckworth-durbin-lead-illinois-democrats-in-demanding-president-trump-immediately-rescind-his-order-to-deploy-national-guard-troops-to-chicago
  [unknown] Publication of merit-based career-level education board selection results, aligning with the restoration of meritocra...
    Official: United States Marine Corps | Date: 2025-10-08 | Conf: 0.75
    URL: https://www.marines.mil/News/Messages/Messages-Display/Article/4322208/fy26-commandants-career-level-education-board-ccleb-selection-results/
  [extends] The President directed use of available Department of War funds to ensure uninterrupted military pay and allowances d...
    Official: President of the United States / The White House | Date: 2025-10-15 | Conf: 0.74
    URL: https://www.whitehouse.gov/presidential-actions/2025/10/national-security-presidential-memorandum-nspm-8/
  [extends] Secretary Hegseth announced tougher standards, training reforms, and further readiness measures for the force.
    Official: Secretary of War Pete Hegseth / U.S. Army Reserve | Date: 2025-10-17 | Conf: 0.93
    URL: https://www.usar.army.mil/News/News-Display/Article/4324195/hegseth-announces-series-of-war-department-reforms-in-sweeping-speech-to-top-mi/
  [unknown] Congresswoman Tenney Applauds President Trump’s Leadership Ensuring Troops Are Paid...
    Official: Representative Claudia Tenney | Date: 2025-10-21 | Conf: 0.75
    URL: https://tenney.house.gov/media/press-releases/congresswoman-tenney-applauds-president-trumps-leadership-ensuring-troops-are
  [extends] Reaffirmed alliance deterrence, expanded bilateral training, and force-posture improvements in Japan as components of...
    Official: Secretary of War Pete Hegseth / U.S. Department of War | Date: 2025-10-30 | Conf: 0.66
    URL: https://www.war.gov/News/Releases/Release/Article/4333197/secretary-of-war-hegseth-reaffirms-strength-of-usjapan-alliance-and-commitment/
  [unknown] National Security Strategy of the United States of America
    Official: The White House | Date: 2025-11 | Conf: 0.90
    URL: https://www.whitehouse.gov/wp-content/uploads/2025/12/2025-National-Security-Strategy.pdf
  [extends] Joint statement reaffirmed combined deterrence, alliance modernization, interoperability, and readiness against DPRK ...
    Official: Joint Chiefs of Staff (U.S.) | Date: 2025-11-03 | Conf: 0.89
    URL: https://www.jcs.mil/Media/News/News-Display/Article/4327660/joint-statement-of-the-50th-republic-of-korea-and-united-states-military-commit/
  [extends] Advanced interoperability, information sharing, multilateral coordination, and humanitarian-response readiness with t...
    Official: Joint Chiefs of Staff | Date: 2025-11-06 | Conf: 0.74
    URL: https://www.jcs.mil/Media/News/News-Display/Article/4328718/readout-of-chairman-of-the-joint-chiefs-of-staff-gen-dan-caines-meeting-with-ph/
  [unknown] Trump Threatens To 'Completely Wipe Out The Islamic Terrorists' Killing Nigerian Christians
    Official: Daily Wire | Date: 2025-11-07 | Conf: 0.75
    URL: https://rileymoore.house.gov/media/in-the-news/daily-wire-trump-threatens-completely-wipe-out-islamic-terrorists-killing
  [extends] The Marine Corps marked its 250th birthday by emphasizing lethality, discipline, honor, readiness, and the enduring w...
    Official: Commandant Gen. Eric M. Smith and Sgt. Maj. Carlos A. Ruiz / United States Marine Corps | Date: 2025-11-10 | Conf: 0.84
    URL: https://www.marines.mil/News/Marines-TV/videoid/307702/?dvpTag=Marine+Corps+Birthday
  [extends] The President connected historical military sacrifice and national strength to a contemporary peace-through-strength ...
    Official: President Donald J. Trump / The White House | Date: 2025-11-11 | Conf: 0.72
    URL: https://www.whitehouse.gov/briefings-statements/2025/11/presidential-message-on-victory-day-for-world-war-i/
  [extends] The United States and the Republic of Korea reaffirmed combined defense posture, readiness, extended deterrence, and ...
    Official: U.S. Department of Defense | Date: 2025-11-14 | Conf: 0.72
    URL: https://media.defense.gov/2025/Nov/14/2003820640/-1/-1/1/57-SECURITY-CONSULTATIVE-MEETING-JOINT-COMMUNIQUE.PDF
  [extends] Formally installed Gen. Ken Wilsbach as Air Force Chief of Staff with a mandate centered on air superiority, innovati...
    Official: Secretary of the Air Force Troy Meink / U.S. Air Force | Date: 2025-11-18 | Conf: 0.81
    URL: https://www.safia.hq.af.mil/IA-News/Article/4337132/wilsbach-formally-installed-as-24th-air-force-chief-of-staff/
  [extends] Republished the installation of Gen. Ken Wilsbach as Chief of Staff with emphasis on readiness, warrior ethos, and a ...
    Official: Air Force Nuclear Weapons Center | Date: 2025-11-18 | Conf: 0.82
    URL: https://www.afnwc.af.mil/News/Article-Display/Article/4337302/wilsbach-formally-installed-as-24th-air-force-chief-of-staff/
  [unknown] Huddle Up
    Official: U.S. Department of War | Date: 2025-11-24 | Conf: 0.60
    URL: https://www.war.gov/Multimedia/Photos/igpage/30/igphoto/2003832562/
  [unknown] Hegseth to Order 500 Additional Guardsmen to D.C. Following Shooting of Two Soldiers
    Official: U.S. Department of War | Date: 2025-11-26 | Conf: 0.85
    URL: https://www.war.gov/News/News-Stories/Article/Article/4343985/hegseth-to-order-500-additional-guardsmen-to-dc-following-shooting-of-two-soldi/
  [unknown] Happy Thanksgiving, Thanks for Serving the Nation, Hegseth Tells Troops During Morale Calls
    Official: U.S. Department of War | Date: 2025-11-26 | Conf: 0.70
    URL: https://www.war.gov/News/News-Stories/Article/Article/4343765/happy-thanksgiving-thanks-for-serving-the-nation-hegseth-tells-troops-during-mo/
  [unknown] During D.C. Guard Visit, Secretary Acknowledges Tragedy, Conveys Gratitude
    Official: U.S. Department of War | Date: 2025-12-01 | Conf: 0.70
    URL: https://www.war.gov/News/News-Stories/Article/Article/4344434/during-dc-guard-visit-secretary-acknowledges-tragedy-conveys-gratitude/
  [implements] Secretary acknowledges tragedy and conveys gratitude during D.C. Guard Visit, reinforcing unit cohesion and morale.
    Official: Air National Guard | Date: 2025-12-01 | Conf: 0.80
    URL: https://www.ang.af.mil/Media/Article-Display/Article/4344587/during-dc-guard-visit-secretary-acknowledges-tragedy-conveys-gratitude/
  [extends] Vice Chairman Gen. Christopher Mahoney visited deployed troops and praised their readiness, resilience, and deterrenc...
    Official: Vice Chairman of the Joint Chiefs of Staff Gen. Christopher Mahoney / U.S. Central Command | Date: 2025-12-02 | Conf: 0.74
    URL: https://www.centcom.mil/MEDIA/igphoto/2003834357/
  [implements] Directed a department-wide barracks task force to improve living conditions as a readiness, morale, and retention issue.
    Official: Secretary of War Pete Hegseth / U.S. Department of War | Date: 2025-12-05 | Conf: 0.78
    URL: https://www.war.gov/News/News-Stories/Article/Article/4350564/hegseth-directs-task-force-to-oversee-departmentwide-barracks-improvement/
  [unknown] Hegseth Outlines New National Defense Strategy During Speech at Reagan Library
    Official: Air Force Nuclear Weapons Center | Date: 2025-12-06 | Conf: 0.90
    URL: https://www.afnwc.af.mil/News/Article-Display/Article/4353970/hegseth-outlines-new-national-defense-strategy-during-speech-at-reagan-library/
  [extends] Outlined a defense strategy built around peace through strength, readiness, restored warrior ethos, deterrence, and A...
    Official: Secretary of War Pete Hegseth / U.S. Department of War | Date: 2025-12-06 | Conf: 0.82
    URL: https://www.war.gov/News/News-Stories/Article/Article/4351527/hegseth-outlines-new-national-defense-strategy-during-speech-at-reagan-library/
  [announces] Published an official editorial presenting an America First defense posture centered on readiness, deterrence, and U....
    Official: Voice of America | Date: 2025-12-08 | Conf: 0.66
    URL: https://editorials.voa.gov/a/charting-an-america-defense-policy/8091478.html
  [legislative codification] Publication of 'THE FY26 NDAA: IMPLEMENTING PRESIDENT TRUMP’S PEACE THROUGH STRENGTH AGENDA', codifying readiness sta...
    Official: House Armed Services Committee | Date: 2025-12-10 | Conf: 0.90
    URL: https://www.war.gov/News/Transcripts/Transcript/Article/4136055/secretary-of-defense-pete-hegseth-holds-media-availability-in-honolulu-hawaii/
  [extends] Trump Awards Soldiers, Marines Border Defense Medal, directly linking military readiness and troop deployments to hom...
    Official: U.S. Department of War / The United States Army | Date: 2025-12-15 | Conf: 0.88
    URL: https://www.war.gov/News/News-Stories/Article/Article/4134243/hegseth-gives-order-to-enhance-military-mission-at-southern-border/
  [implements] Awarding Soldiers and Marines the Border Defense Medal, linking military readiness directly to homeland protection.
    Official: U.S. Department of War | Date: 2025-12-15 | Conf: 0.95
    URL: https://www.war.gov/News/News-Stories/Article/Article/4360811/trump-awards-soldiers-marines-border-defense-medal/
  [extends] President connected border service to homeland defense, operational readiness, and military recognition through prese...
    Official: Office of the Federal Register, National Archives and Records Administration | Date: 2025-12-15 | Conf: 0.78
    URL: https://www.govinfo.gov/app/details/DCPD-202501195
  [implements] Awarding Soldiers and Marines the Border Defense Medal.
    Official: The United States Army | Date: 2025-12-16 | Conf: 0.95
    URL: https://www.army.mil/article/289623/trump_awards_soldiers_marines_border_defense_medal
  [extends] The Justice Department endorsed elevating spiritual well-being and strengthening the Chaplain Corps as part of force ...
    Official: U.S. Department of Justice | Date: 2025-12-17 | Conf: 0.69
    URL: https://www.justice.gov/opa/blog/statement-department-wars-strengthening-chaplain-corps
  [announces] Senior civilian and military leaders honored soldiers and an interpreter killed in Syria, pairing the ceremony with s...
    Official: U.S. Department of War | Date: 2025-12-17 | Conf: 0.73
    URL: https://www.war.gov/News/News-Stories/Article/Article/4363212/trump-hegseth-caine-nordhaus-honor-soldiers-interpreter-killed-in-syria/
  [implements] Rendering Salutes
    Official: U.S. Department of War | Date: 2025-12-17 | Conf: 0.75
    URL: https://www.war.gov/Multimedia/Photos/igphoto/2003847764/
  [extends] The War Department held a Recruitment Excellence Forum, honored top recruiters, and treated recruiting performance as...
    Official: Secretary of War Pete Hegseth / U.S. Department of War | Date: 2025-12-18 | Conf: 0.88
    URL: https://www.war.gov/News/News-Stories/Article/Article/4364310/hegseth-senior-leaders-honor-militarys-top-recruiters/
  [extends] Gifting service members a $1,776 'Warrior Dividend' to boost morale and reward readiness.
    Official: U.S. Department of War | Date: 2025-12-18 | Conf: 0.90
    URL: https://www.war.gov/News/News-Stories/Article/Article/4363528/just-in-time-for-christmas-nation-gifts-service-members-1776-warrior-dividend/
  [extends] Gifting service members a $1,776 'Warrior Dividend'.
    Official: The United States Army | Date: 2025-12-18 | Conf: 0.90
    URL: https://www.army.mil/article/289703/just_in_time_for_christmas_nation_gifts_service_members_1776_warrior_dividend
  [implements] Statement by the President regarding military readiness and the Warrior Dividend.
    Official: The White House | Date: 2025-12-18 | Conf: 0.85
    URL: https://www.whitehouse.gov/briefings-statements/2025/12/statement-by-the-president-7598/
  [extends] Just In Time For Christmas, Nation Gifts Service Members $1776 'Warrior Dividend'
    Official: U.S. Army Financial Management Command | Date: 2025-12-18 | Conf: 0.75
    URL: https://www.usafmcom.army.mil/News/Article-Display/Article/4364861/just-in-time-for-christmas-nation-gifts-service-members-1776-warrior-dividend/
  [extends] Recognized top Air Force and Space Force recruiters and used the Recruiting Excellence Forum to spread best practices...
    Official: Joint Base San Antonio | Date: 2025-12-18 | Conf: 0.89
    URL: https://www.jbsa.mil/News/News/Article/4369361/secretary-of-war-honors-top-usaf-ussf-recruiters/
  [implements] Secretary of War Honors Top Recruiter's
    Official: Air Force Accessions Center Public Affairs | Date: 2025-12-19 | Conf: 0.92
    URL: https://www.afaccessionscenter.af.mil/News/Article-Display/Article/4364836/secretary-of-war-honors-top-recruiters/
  [extends] Highlighted the Warrior Dividend and the Mexican Border Defense Medal as measures supporting morale, recognition, and...
    Official: U.S. Department of War | Date: 2025-12-20 | Conf: 0.72
    URL: https://www.war.gov/News/News-Stories/Article/Article/4365527/this-week-in-dow-border-defense-medal-warrior-dividend-reforming-chaplain-corps/
  [extends] Secretary Hegseth made global morale calls to deployed personnel and emphasized troop support and retention during th...
    Official: Secretary of War Pete Hegseth / U.S. Department of War | Date: 2025-12-22 | Conf: 0.76
    URL: https://www.war.gov/News/News-Stories/Article/Article/4366534/hegseth-makes-christmas-morale-calls-to-service-members-across-the-globe/
  [implements] Reported record fiscal year 2025 recruiting and detailed restructuring of recruiter support and processes in the name...
    Official: Air National Guard | Date: 2025-12-26 | Conf: 0.95
    URL: https://www.ang.af.mil/Media/Article-Display/Article/4367469/ang-touts-key-changes-after-record-setting-recruitment-year/
  [unknown] Lethal Kinetic Strikes, Dec. 31, 2025
    Official: U.S. Southern Command | Date: 2025-12-31 | Conf: 0.85
    URL: https://www.southcom.mil/News/PressReleases/Article/4369758/lethal-kinetic-strikes-dec-31-2025/
  [unknown] Trump Announces U.S. Military's Capture of Maduro / Operation Absolute Resolve
    Official: U.S. Department of War / The White House | Date: 2026-01-03 | Conf: 0.88
    URL: https://www.war.gov/News/News-Stories/Article/Article/4370431/trump-announces-us-militarys-capture-of-maduro/
  [implements] Swearing in of new recruits under the restored, rigorous fitness and merit-based standards, emphasizing the 'warrior ...
    Official: Secretary of War Pete Hegseth / U.S. Department of War | Date: 2026-01-08 | Conf: 0.90
    URL: https://www.war.gov/News/News-Stories/Article/Article/4374025/hegseth-swears-in-40-west-coast-recruits-into-military/
  [extends] Reported a SOUTHCOM maritime interdiction under Operation Southern Spear, reflecting continued operational employment...
    Official: U.S. Southern Command | Date: 2026-01-15 | Conf: 0.55
    URL: https://www.southcom.mil/News/PressReleases/Article/4380252/maritime-interdiction-operation-jan-15-2026/
  [extends] Reported IRS confirmation that the $1,776 Warrior Dividend paid to service members was tax-free, reinforcing morale a...
    Official: Pentagon Press Secretary Kingsley Wilson / U.S. Department of War | Date: 2026-01-16 | Conf: 0.58
    URL: https://www.war.gov/News/News-Stories/Article/Article/4382089/1776-warrior-dividend-tax-free-irs-confirms/
  [implements] Modification of training timelines to accelerate readiness and deployment capabilities, aligning with the directive t...
    Official: National Guard | Date: 2026-01-21 | Conf: 0.85
    URL: https://www.nationalguard.mil/News/Article-View/Article/4383607/washington-guard-shortens-officer-candidate-school-by-four-months/
  [implements] Release of the 2026 National Defense Strategy, reorienting the military on lethality, readiness, and warfighting.
    Official: Department of War | Date: 2026-01-23 | Conf: 0.98
    URL: https://media.defense.gov/2026/Jan/23/2003864773/-1/-1/0/2026-NATIONAL-DEFENSE-STRATEGY.PDF
  [extends] Used Operation Absolute Resolve and the capture of Nicolás Maduro to argue that U.S. deterrence and military credibil...
    Official: Secretary of War Pete Hegseth / U.S. Department of War | Date: 2026-01-29 | Conf: 0.75
    URL: https://www.war.gov/News/News-Stories/Article/Article/4393256/hegseth-touts-deterrent-effect-of-venezuela-raid-during-first-2026-cabinet-meet/
  [unknown] Hegseth tasks Army to transform to leaner, more lethal force
    Official: The United States Army | Date: 2026-02-06 | Conf: 0.95
    URL: https://www.army.mil/article/285106/hegseth_tasks_army_to_transform_to_leaner_more_lethal_force
  [extends] Reenlisted more than 100 National Guardsmen and publicly tied Guard retention and morale to mission success and readi...
    Official: U.S. Department of War | Date: 2026-02-06 | Conf: 0.71
    URL: https://www.war.gov/News/News-Stories/Article/Article/4399570/hegseth-reenlists-more-than-100-national-guardsmen-in-nations-capital/
  [extends] Weekly roundup highlighted recruiting gains and broader readiness themes across the force.
    Official: U.S. Department of War | Date: 2026-02-06 | Conf: 0.69
    URL: https://www.war.gov/News/News-Stories/Article/Article/4398892/this-week-in-dow-arsenal-of-freedom-tour-looks-to-space-protecting-vmi-scouting/
  [] As part of the 'Arsenal of Freedom' tour, Secretary of War Pete Hegseth visited defense industrial base facilities in...
    Official: Secretary of War | Date: 2026-02-11 | Conf: 0.00
    URL: https://www.war.gov/News/Tag/81196/trump/
  [extends] Testified that housing, healthcare, childcare, spouse employment, and financial stability are foundational to readine...
    Official: Senior Enlisted Advisor to the Chairman David L. Isom / U.S. Department of War | Date: 2026-02-12 | Conf: 0.80
    URL: https://www.war.gov/News/News-Stories/Article/Article/4404617/senior-enlisted-advisor-to-chairman-testifies-on-quality-of-life/
  [] President Donald J. Trump made general officer nominations, announced by Secretary of War Pete Hegseth.
    Official: Secretary of War | Date: 2026-02-12 | Conf: 0.00
    URL: https://www.war.gov/News/?Search=Publicaci%C3%B3n+de+la+Casa+Blanca+y+del+presidente+Donald+Trump+sobre+Venezuela+-+U.S.+Department+of+State+(.gov)&Page=6
  [announces] Announced troop physical training and a 101st Airborne address focused on resilience, character, and combat readiness.
    Official: Secretary of War Pete Hegseth / U.S. Department of War | Date: 2026-02-19 | Conf: 0.86
    URL: https://www.war.gov/News/Advisories/Advisory/Article/4409625/secretary-of-war-pete-hegseth-travels-to-nashville-tn-and-fort-campbell-ky/
  [implements] The Department of the Air Force executed a large integrated exercise series focused on building warfighters, restorin...
    Official: Department of the Air Force | Date: 2026-02-19 | Conf: 0.90
    URL: https://www.af.mil/DLE/?videoid=992829&dvpmoduleid=5710&dvpTag=DGOVLIVE
  [extends] Recognized top performers whose work sustains enterprise-wide Air Force readiness and maintenance capacity.
    Official: Oklahoma City Air Logistics Complex / Tinker Air Force Base | Date: 2026-02-23 | Conf: 0.58
    URL: https://www.tinker.af.mil/News/Article-Display/Article/4412386/oc-alc-honors-top-performers-at-annual-awards-ceremony/
  [extends] The State of the Union military-honors ceremony reinforced valor, sacrifice, and a public-facing warrior-ethos narrat...
    Official: President Donald J. Trump / U.S. Department of War | Date: 2026-02-24 | Conf: 0.78
    URL: https://www.war.gov/News/News-Stories/Article/Article/4413689/two-american-heroes-awarded-medal-of-honor-during-state-of-the-union/
  [extends] Explained how National Guard posture, recruitment, and mission priorities aligned with the National Defense Strategy’...
    Official: Secretary of War Pete Hegseth / U.S. Department of War | Date: 2026-02-25 | Conf: 0.89
    URL: https://www.war.gov/News/News-Stories/Article/Article/4414722/hegseth-national-guard-aligned-with-national-defense-strategy-homeland-first-bo/
  [extends] The Navy inducted Medal of Honor recipient Elmer Royce Williams into the Pentagon Hall of Heroes, publicly emphasizin...
    Official: Secretary of War Pete Hegseth; Secretary of the Navy John Phelan; Adm. Daryl Caudle / United States Navy | Date: 2026-02-25 | Conf: 0.81
    URL: https://www.navy.mil/Press-Office/News-Stories/display-news/Article/4414311/secretary-of-war-inducts-naval-aviator-and-medal-of-honor-recipient-into-the-pe/
  [announces] The Marine Corps formally announced officer promotions effective March 2026 and projected April 2026 promotions under...
    Official: Commandant of the Marine Corps / Manpower and Reserve Affairs, Manpower Management (MRA MM) | Date: 2026-02-25 | Conf: 0.31
    URL: https://www.marines.mil/News/Messages/Messages-Display/Article/4414032/officer-promotions-for-march-2026-and-projected-officer-promotions-for-april-20/
  [announces] Published a weekly recap highlighting military honors, patrol operations, and public examples of force strength and o...
    Official: U.S. Department of War | Date: 2026-02-27 | Conf: 0.61
    URL: https://www.war.gov/News/News-Stories/Article/Article/4418161/this-week-in-dow-honoring-heroes-patrolling-oceans-uncovering-truth-out-there/
  [extends] NAVSEA and NSWCPD integrated Sailor feedback through realistic AR/VR testing to improve the Advanced Weapons Elevator...
    Official: Naval Sea Systems Command (NAVSEA) / Naval Surface Warfare Center, Philadelphia Division | Date: 2026-02-27 | Conf: 0.58
    URL: https://www.navsea.navy.mil/Media/News/Article-View/Article/4417589/sailors-feedback-helps-shape-carrier-performance/
  [extends] Announced the launch of Operation Epic Fury as a major combat operation framed around peace through strength and deci...
    Official: President Donald J. Trump / The White House | Date: 2026-03-01 | Conf: 0.76
    URL: https://www.whitehouse.gov/articles/2026/03/peace-through-strength-president-trump-launches-operation-epic-fury-to-crush-iranian-regime-end-nuclear-threat/
  [announces] Official leadership profile emphasized Gen. Dan Caine's priorities of proper arming, modernization, global integratio...
    Official: U.S. Department of War | Date: 2026-03-02 | Conf: 0.67
    URL: https://www.war.gov/About/Chairman-of-the-Joint-Chiefs-of-Staff/
  [extends] Summarized sustained combat operations against Iran as evidence of decisive combat power, force readiness, and missio...
    Official: Pentagon Press Secretary Kingsley Wilson / U.S. Department of War | Date: 2026-03-20 | Conf: 0.79
    URL: https://www.war.gov/News/News-Stories/Article/Article/4440215/this-week-in-dow-us-delivers-devastating-combat-power-to-iran/
  → 68 linked media articles
  → 68 unique linked media URLs
  → Unlinked media: DISABLED (use --include-unlinked to enable)
  → 68 total unique media URLs

[Phase 1 Complete]
  ✓ 181 primary URLs (workflow cache, confidence ≥ 0.00)
  ✓ 68 linked media URLs (BBAO)
  ✓ 68 total media URLs
  ✓ 213 total unique URLs for Phase 2

======================================================================
Phase 2: Category Grouping and Graph Expansion
======================================================================
| 2026-04-04 11:41:54,413 | INFO - CategoryGrouper - Category 'culture_and_media': 10 anchors → 10 total EOs (depth 2)
| 2026-04-04 11:41:54,413 | INFO - CategoryGrouper - Category 'artificial_intelligence': 5 anchors → 5 total EOs (depth 2)
| 2026-04-04 11:41:54,414 | INFO - CategoryGrouper - Category 'federal_procurement': 5 anchors → 5 total EOs (depth 2)
| 2026-04-04 11:41:54,414 | INFO - CategoryGrouper - Category 'energy_and_minerals': 4 anchors → 4 total EOs (depth 2)
| 2026-04-04 11:41:54,414 | INFO - CategoryGrouper - Category 'national_security_domestic': 6 anchors → 6 total EOs (depth 2)
| 2026-04-04 11:41:54,414 | INFO - CategoryGrouper - Category 'national_security_foreign': 8 anchors → 8 total EOs (depth 2)
| 2026-04-04 11:41:54,414 | INFO - CategoryGrouper - 1 EOs appear in multiple categories

[Category Expansion] read_with edges only (depth 2):

  culture_and_media:
    10 anchors → 0 discovered = 10 total EOs
    10 EOs have BBAO events, 409 workflow URLs
    Anchor EOs:
      • 13950: Combating Race and Sex Stereotyping
      • 13963: Providing an Order of Succession Within the Department of Defense
      • 13989: Ethics Commitments by Executive Branch Personnel
      • 14148: Initial Rescissions of Harmful Executive Orders and Actions
      • 14151: Ending Radical and Wasteful Government DEI Programs and Preferencing
      • 14173: Ending Illegal Discrimination and Restoring Merit-Based Opportunity
      • 14183: Prioritizing Military Excellence and Readiness
      • 14184: Reinstating Service Members Discharged Under the Military's COVID-19 Vaccination Mandate
      • 14185: Restoring America's Fighting Force
      • 14347: Restoring the United States Department of War

  artificial_intelligence:
    5 anchors → 0 discovered = 5 total EOs
    4 EOs have BBAO events, 38 workflow URLs
    Anchor EOs:
      • 13859: Unknown
      • 13960: Unknown
      • 14110: Unknown
      • 14179: Removing Barriers to American Leadership in Artificial Intelligence
      • 14319: Preventing Woke AI in the Federal Government

  federal_procurement:
    5 anchors → 0 discovered = 5 total EOs
    5 EOs have BBAO events, 202 workflow URLs
    Anchor EOs:
      • 14265: Modernizing Defense Acquisitions and Spurring Innovation in the Defense Industrial Base
      • 14268: Reforming Foreign Defense Sales To Improve Speed and Accountability
      • 14307: Unleashing American Drone Dominance
      • 14372: Prioritizing the Warfighter in Defense Contracting
      • 14383: Establishing an America First Arms Transfer Strategy

  energy_and_minerals:
    4 anchors → 0 discovered = 4 total EOs
    4 EOs have BBAO events, 46 workflow URLs
    Anchor EOs:
      • 14154: Unleashing American Energy
      • 14192: Unleashing Prosperity Through Deregulation
      • 14241: Immediate Measures To Increase American Mineral Production
      • 14387: Promoting the National Defense by Ensuring an Adequate Supply of Elemental Phosphorus and Glyphosate-Based Herbicides

  national_security_domestic:
    6 anchors → 0 discovered = 6 total EOs
    5 EOs have BBAO events, 148 workflow URLs
    Anchor EOs:
      • 14157: Designating Cartels and Other Organizations as Foreign Terrorist Organizations and Specially Designated Global Terrorists
      • 14161: Protecting the United States From Foreign Terrorists and Other National Security and Public Safety Threats
      • 14165: Securing Our Borders
      • 14167: Clarifying the Military's Role in Protecting the Territorial Integrity of the United States
      • 14269: Restoring America's Maritime Dominance
      • 14367: Designating Fentanyl as a Weapon of Mass Destruction

  national_security_foreign:
    8 anchors → 0 discovered = 8 total EOs
    8 EOs have BBAO events, 82 workflow URLs
    Anchor EOs:
      • 14175: Designation Of Ansar Allah as a Foreign Terrorist Organization
      • 14186: The Iron Dome for America
      • 14199: Withdrawing the United States from and Ending Funding to Certain United Nations Organizations and Reviewing United States Support to All International Organizations
      • 14305: Restoring American Airspace Sovereignty
      • 14346: Modifying the Scope of Reciprocal Tariffs and Establishing Procedures for Implementing Trade and Security Agreements
      • 14357: Unknown
      • 14369: Ensuring American Space Superiority
      • 14383: Establishing an America First Arms Transfer Strategy

[Phase 2 Complete]
  ✓ 38 manifest entries
  ✓ Categories ready for Phase 3 reconstitution

======================================================================
Phase 3: Full-Doc Reconstitution [culture_and_media]
======================================================================

[Step 1: Extract URLs from Workflow Cache]
  → Processing 10 EOs: 13950, 13963, 13989, 14148, 14151, 14173, 14183, 14184, 14185, 14347
  → 519 workflow cache records (communications about these EOs)
  → 409 UNIQUE URLs extracted (some URLs appear in multiple records)

[Step 2: Get Workflow Media Articles with Rationale]
  → 252 workflow articles found
  → 252 UNIQUE URLs from workflow cache

[Step 3: Enrich with BBOA Media Framing (Optional)]
  → BBOA cache: 2,161 articles with media_framing available
  → 199/252 workflow articles enriched (79.0%)

[Workflow Evidence Package]
  → 252 total articles from workflow
  → 252 with linkage rationale
  → 223 with relevant events
  → 199 with media framing (BBOA enrichment)

[URL Summary]
  → 409 primary URLs (workflow EO cache)
  → 252 media URLs (workflow media linking cache)
  → 660 TOTAL unique URLs for reconstitution

[Step 5: RAG Reconstitution - DRY RUN MODE]

  ╔══════════════════════════════════════════════════════════════╗
  ║  LIVE RECONSTITUTION SAMPLE (1 article via RAG)             ║
  ╚══════════════════════════════════════════════════════════════╝
  URL:            https://www.sgvtribune.com/2025/01/07/elizabeth-warren-pete-hegseth-questions/
  Title:          Sen. Warren presses Hegseth to answer questions about his past actions and statements
  QA pairs from RAG:  17
  Unique chunks after dedup: 8
  Full text length:   6,367 chars

  --- Reconstituted Text (first 2 chunks) ---
  [1] - Log In [1] Spoof a user - Log In [1] Monday, March 2nd 2026 - Investigative Reporting [2] - Vote for Readers' Choice 🗳 [3] - Weather news [4] - Immigration news [5] - West Covina sues councilman [6] - High school sports [7]
  [2] Sen. Warren presses Hegseth to answer questions about his past actions and statements: # Sen. Warren presses Hegseth to answer questions about his past actions and statements

  → Building synthetic docs for 660 URLs (no actual RAG calls)
  ✓ 660 documents created (1 live, rest synthetic)

[Step 6: Build Analysis Package]
  → Packaging 252 workflow media articles (enriched with BBOA framing where available)...
  ✓ 252 total articles packaged for analysis
    • 252 with full-text (reconstituted via RAG)
    • 0 metadata-only (title, URL, BBAO tags)

================================================================================
SAMPLE RECONSTITUTED ARTICLE (FULL) - 1 of 252
================================================================================

--- ARTICLE METADATA ---
  URL:                  https://www.sgvtribune.com/2025/01/07/elizabeth-warren-pete-hegseth-questions/
  Title:                Sen. Warren presses Hegseth to answer questions about his past actions and statements
  Author:               Tara Copp, Associated Press
  Publisher:             San Gabriel Valley Tribune
  Listed Publisher:      San Gabriel Valley Tribune
  Publication Date:      2025-01-07
  Source Type:           linked
  EO Identifier:         14183
  Event Key:             None
  Consumer Salience:     None
  Mean Search Rank:      0.0
  Reconstituted:         True
  QA Pairs Used:         17

--- RAG DOCUMENT METADATA ---
  title: Sen. Warren presses Hegseth to answer questions about his past actions and statements
  authors: ['Tara Copp', 'Associated Press']
  publication_date: 2025-01-07
  publisher: San Gabriel Valley Tribune
  publication_venue: sgvtribune.com
  type: other

Reconstitution Source: 17 QA context chunks (deduplicated)

--- FULL TEXT ---
- Log In [1] Spoof a user - Log In [1] Monday, March 2nd 2026 - Investigative Reporting [2] - Vote for Readers' Choice 🗳 [3] - Weather news [4] - Immigration news [5] - West Covina sues councilman [6] - High school sports [7]

Sen. Warren presses Hegseth to answer questions about his past actions and statements: # Sen. Warren presses Hegseth to answer questions about his past actions and statements

## Warren questioned if Hegseth would be able to lead, saying she was "deeply concerned by the many ways in which your past behavior and rhetoric indicates that you are unfit to lead the Department of Defense."

Pete Hegseth, President-elect Donald Trump's nominee to be Defense Secretary, walks to meet with senators, at the Capitol in Washington, Tuesday, Dec. 17, 2024. (AP Photo/J. Scott Applewhite) By Associated Press [8] PUBLISHED: January 7, 2025 at 2:44 PM PST | UPDATED: January 7, 2025 at 3:58 PM PST **Getting your** **Trinity Audio** [9] **player ready...** **By TARA COPP** WASHINGTON (AP) - Sen. Elizabeth Warren is pressing Pete Hegseth, President-elect Donald Trump's pick [10] for defense secretary, to answer additional questions about his past actions and statements before next Tuesday's confirmation hearing. - Melania Trump will preside at UN Security Council meeting on children in conflict as US attacks Iran [11] - A Supreme Court case over whether marijuana users can own guns is creating unusual alliances [12] - Trump expects his Fed pick and AI to deliver a replay of the '90s boom. Economists have doubts [13] - Where things stand after the US and Israeli strikes on Iran [14] - Trump awards the Medal of Honor to 3 US Army service members in a White House ceremony [15] the Massachusetts Democrat asked [16] In a letter to Hegseth sent Monday, the former Fox News host and Army veteran to respond no later than Friday to questions on allegations of past drinking, and mismanagement of veterans organization finances - all of which he denies. She also wants to know about statements he's made concerning minorities, women and LGBTQ people serving in the military. Hegseth has the support of some veterans groups that believe the military has overemphasized diversity at the cost of troop readiness. A group of former Navy SEALs is planning a rally in support of Hegseth in Washington next week. "One of the biggest flaws in current leadership in the military and in the Pentagon is when you become more focused on things that don't matter," said Bill Brown, a former SEAL who is organizing Tuesday's rally. "The military is not a social justice project." Hegseth's supporters have stressed that many of the questions about his past behavior arose from reports based on anonymous sources.

Hegseth's supporters have stressed that many of the questions about his past behavior arose from reports based on anonymous sources. But some relate to things he's said in interviews or written in his books. In "War on Warriors," Hegseth wrote that Gen. CQ Brown's promotion to chairman of the Joint Chiefs of Staff meant that "with the Pentagon now run by, and fully staffed by, so-called 'leaders' like CQ Brown, we can assume that 17 percent of all black officers in the Air Force are simply promoted because of how they look - and not how they lead." Hegseth also has openly criticized the role of women in combat, although in meetings with senators he seemed to walk back some of those views. Warren questioned if Hegseth would be able to lead, saying she was "deeply concerned by the many ways in which your past behavior and rhetoric indicates that you are unfit to lead the Department of Defense." A spokesperson for the Trump transition team did not immediately respond to a request for comment. The Trust Project logo - Newsroom Guidelines [17] - Report an Error [18] - Thousands brace for fallout from unreported LA County criminal convictions Thousands brace for fallout from unreported LA County criminal convictions [19] - Santa Anita consensus picks for Sunday, March 1, 2026 Santa Anita consensus picks for Sunday, March 1, 2026 [20] - Could Puente Hills Mall become a data center? City, developer are discussing it Could Puente Hills Mall become a data center? City, developer are discussing it [21] - Bishop Amat defeats Newport Harbor to earn back-to-back boys soccer titles Bishop Amat defeats Newport Harbor to earn back-to-back boys soccer titles [22] - Bishop Amat overwhelms Hesperia to win Division 2 boys basketball title Bishop Amat overwhelms Hesperia to win Division 2 boys basketball title [23] - LAPD investigating 12-year-old girl's death after bully-related attack at Reseda school LAPD investigating 12-year-old girl's death after bully-related attack at Reseda school [24] - Sierra Vista girls basketball tops Desert Hot Springs to win first CIF-SS title Sierra Vista girls basketball tops Desert Hot Springs to win first CIF-SS title [25]

- Rio Hondo Prep boys soccer wins Division 8 title in thrilling finish Rio Hondo Prep boys soccer wins Division 8 title in thrilling finish [26] - 1 killed, 2 injured near Nipsey Hussle's Marathon Burger in Long Beach after grand opening 1 killed, 2 injured near Nipsey Hussle's Marathon Burger in Long Beach after grand opening [27] - South El Monte basketball's progress halted by Victor Valley in CIF-SS title game South El Monte basketball's progress halted by Victor Valley in CIF-SS title game [28]

## More in National Politics

- Federal court rejects Trump administration attempt to slow tariff refund process [29] FILE-A FedEx cargo plane is shown on the tarmac at Fort Lauderdale-Hollywood International Airport, Tuesday, April 20, 2021, in Fort Lauderdale, Fla. (AP Photo/Wilfredo Lee, File) - Melania Trump will preside at UN Security Council meeting on children in conflict as US attacks Iran [11] First lady Melania Trump arrives before President Donald Trump speaks to soldiers and their families at Fort Bragg, N.C., Friday, Feb. 13, 2026. (AP Photo/Matt Rourke) - A Supreme Court case over whether marijuana users can own guns is creating unusual alliances [12] The Supreme Court is seen. - Trump expects his Fed pick and AI to deliver a replay of the '90s boom. Economists have doubts [13] FILE - Kevin Warsh speaks to the media about his report on transparency at the Bank of England, in London, Dec. 11, 2014. (AP Photo/Alastair Grant, Pool, File)

--- BBOA MEDIA FRAMING (Enrichment) ---
  • factualness: The article appears factually sound, reporting on a publicly sent letter from Senator Warren, quoting directly from the nominee's published book, and noting denials from the nominee. It accurately represents the key points of contention in a public political dispute, including the perspectives of the nominee's supporters and the lack of comment from the transition team.
  • spin: The article's primary spin is through selection and framing, leading with Senator Warren's challenge in the headline and opening paragraphs. While it includes counterpoints from Hegseth's supporters, the narrative is structured around the 72 questions and Warren's direct quote that the nominee is "unfit to lead," thereby centering the opposition's perspective.
  • convergence_divergence: As this is standalone reporting, it is assessed against a neutral baseline. The article converges with the public record by accurately citing Warren's letter and Hegseth's past statements. It diverges from a purely neutral presentation by dedicating significantly more space and detail to the criticisms leveled by Senator Warren than to the defense offered by Hegseth's supporters.
  • supportive_unsupportive: The article is unsupportive of the administration's nominee. It extensively details and amplifies a prominent Democratic senator's criticisms regarding the nominee's past conduct and statements on diversity, framing him as unfit for office. By focusing on these criticisms, which are rooted in DEI-related concerns, the article promotes a narrative that challenges the administration's personnel choice and runs counter to its stated merit-based policy agenda.
  • linked_to_dow: The article is directly linked to the Department of Defense as it covers the contentious confirmation process for the Secretary of Defense nominee, Pete Hegseth. It scrutinizes his qualifications, past statements on military personnel policy, and potential ethical conflicts, all of which are central to his fitness to lead the Pentagon.
  • risk: {'rating': 'HIGH', 'reasoning': "The risk is rated HIGH because the article contains specific allegations of potential criminal and civil liability, meeting the strict gate for this rating. The text explicitly references Senator Warren's letter containing 'allegations of excessive drinking, sexual assault, and financial mismanagement,' which could cause 'damage' to national security by destabilizing the DoD confirmation process, mapping to the Confidential tier under EO 13526. Because the article directly reports specific allegations of liability, the doubt rule was not applied to downgrade the rating to Medium."}
================================================================================

[Phase 3 Complete]
  ✓ 660 URLs traced: workflow cache → reconstitution
  ✓ Analysis package ready with 252 articles for Phase 4
  ✓ Evidence sources: workflow media linking cache (primary), BBOA (enrichment)
  ✓ 519 EO grounding events extracted for LLM context

[Step 7: EO Policy Reconstitution]
  → 10 EOs mapped to Federal Register URLs
  → DRY RUN: building synthetic EO policy texts
  ✓ 10 synthetic EO policy texts created
    • EO 13950: Combating Race and Sex Stereotyping (70 chars)
    • EO 13963: Providing an Order of Succession Within the Department of De (100 chars)
    • EO 13989: Ethics Commitments by Executive Branch Personnel (83 chars)
    • EO 14148: Initial Rescissions of Harmful Executive Orders and Actions (94 chars)
    • EO 14151: Ending Radical and Wasteful Government DEI Programs and Pref (103 chars)
    • EO 14173: Ending Illegal Discrimination and Restoring Merit-Based Oppo (102 chars)
    • EO 14183: Prioritizing Military Excellence and Readiness (81 chars)
    • EO 14184: Reinstating Service Members Discharged Under the Military's  (123 chars)
    • EO 14185: Restoring America's Fighting Force (69 chars)
    • EO 14347: Restoring the United States Department of War (80 chars)

======================================================================
BM25 Grounding Injection [culture_and_media]
======================================================================
  Loaded 2127 tagged articles ({'unlinked': 1431, 'linked': 696})
  Running 6 targeted BM25 queries for [culture_and_media]
    Q1: 100 hits, 46 new  (topic='culture_and_media', person='Hegseth')
    Q2: 100 hits, 93 new  (topic='culture_and_media', person='JD Vance')
    Q3: 100 hits, 78 new  (topic='culture_and_media', person='Dan Caine')
    Q4: 100 hits, 49 new  (topic='culture_and_media', person='Steven Feinberg')
    Q5: 100 hits, 29 new  (topic='culture_and_media', person='Christopher Mahoney')
    Q6: 100 hits, 7 new  (topic='culture_and_media', person='Robert Salesses')
  → 302 new articles injected (deduplicated against 252 existing)
  → Bundle now: 554 total articles

[BM25 Reconstitution - DRY RUN]
  → 302 BM25 articles given synthetic reconstitution

  → Loaded n-1 prior analysis: 324,499 chars

======================================================================
Phase 4: Threat Cluster Analysis [culture_and_media]
======================================================================

[Source Filtering — Domain Greylist]
  → 554 total articles in evidence bundle
  → 5 greylisted sources EXCLUDED:
    • www.britannica.com: 2
    • www.ngv.vic.gov.au: 2
    • www.screenaustralia.gov.au: 1
  → 549 media articles after greylist filter
    • 250 linked articles (all included)
    • 299 BM25-queried articles (all included)
    • 0 remaining unlinked articles,  window=no linked dates — no filter, 0/0 in window
    • 0 randomly sampled from date window (+150 buffer)
  → 549 total articles sent to LLM

[Article Enrichment]
  → 0 articles with linkage rationale
  → 0 articles with relevant events
  → 386 articles with media framing

[Context Injection]
  → 519 EO grounding events injected into prompt
  → 10 EO policy documents injected into prompt
  → Prior analysis (n-1) injected: 324,499 chars

[Article Char Counts — Ranked by full_text length]
    1.    6,367 chars [linked  ] www.sgvtribune.com             Sen. Warren presses Hegseth to answer questions about his past actions and state
    2.       55 chars [linked  ] www.publishersweekly.com       [Synthetic] Article from https://www.publishersweekly.com/pw/by-topic/indus
    3.       55 chars [linked  ] www.politico.com               [Synthetic] Article from https://www.politico.com/live-updates/2025/01/08/c
    4.       55 chars [linked  ] thehill.com                    [Synthetic] Article from https://thehill.com/opinion/national-security/5072
    5.       55 chars [linked  ] www.detroitnews.com            [Synthetic] Article from https://www.detroitnews.com/story/news/politics/20
    6.       55 chars [linked  ] thehill.com                    [Synthetic] Article from https://thehill.com/policy/defense/5238924-top-nat
    7.       55 chars [linked  ] www.nbcnews.com                [Synthetic] Article from https://www.nbcnews.com/politics/national-security
    8.       55 chars [linked  ] www.theguardian.com            [Synthetic] Article from https://www.theguardian.com/us-news/2025/apr/15/pe
    9.       55 chars [linked  ] www.forbes.com                 [Synthetic] Article from https://www.forbes.com/sites/danalexander/2025/04/
   10.       55 chars [linked  ] www.politico.com               [Synthetic] Article from https://www.politico.com/news/2025/04/29/hegseth-w
   11.       55 chars [linked  ] www.nytimes.com                [Synthetic] Article from https://www.nytimes.com/2025/05/08/opinion/west-po
   12.       55 chars [linked  ] www.reuters.com                [Synthetic] Article from https://www.reuters.com/world/us/us-military-start
   13.       55 chars [linked  ] www.theguardian.com            [Synthetic] Article from https://www.theguardian.com/us-news/2025/may/09/tr
   14.       55 chars [linked  ] www.nytimes.com                [Synthetic] Article from https://www.nytimes.com/2025/05/20/us/politics/heg
   15.       55 chars [linked  ] www.washingtonpost.com         [Synthetic] Article from https://www.washingtonpost.com/national-security/2
   16.       55 chars [linked  ] www.politico.com               [Synthetic] Article from https://www.politico.com/news/magazine/2025/05/25/
   17.       55 chars [linked  ] www.theatlantic.com            [Synthetic] Article from https://www.theatlantic.com/politics/archive/2025/
   18.       55 chars [linked  ] www.independent.co.uk          [Synthetic] Article from https://www.independent.co.uk/news/world/americas/
   19.       55 chars [linked  ] www.independent.co.uk          [Synthetic] Article from https://www.independent.co.uk/news/world/americas/
   20.       55 chars [linked  ] www.theguardian.com            [Synthetic] Article from https://www.theguardian.com/us-news/2025/jul/20/pe
   21.       55 chars [linked  ] www.nytimes.com                [Synthetic] Article from https://www.nytimes.com/live/2025/06/26/us/trump-n
   22.       55 chars [linked  ] www.theatlantic.com            [Synthetic] Article from https://www.theatlantic.com/national-security/arch
   23.       55 chars [linked  ] www.newsweek.com               [Synthetic] Article from https://www.newsweek.com/we-dont-believe-erasing-a
   24.       55 chars [linked  ] thehill.com                    [Synthetic] Article from https://thehill.com/policy/defense/5469171-trump-r
   25.       55 chars [linked  ] www.politico.com               [Synthetic] Article from https://www.politico.com/news/2025/08/25/trump-pen
   26.       55 chars [linked  ] www.aol.com                    [Synthetic] Article from https://www.aol.com/trump-says-secretary-hegseth-w
   27.       55 chars [linked  ] www.forbes.com                 [Synthetic] Article from https://www.forbes.com/sites/petersuciu/2025/08/25
   28.       55 chars [linked  ] www.washingtonpost.com         [Synthetic] Article from https://www.washingtonpost.com/history/2025/08/26/
   29.       55 chars [linked  ] www.independent.co.uk          [Synthetic] Article from https://www.independent.co.uk/news/world/americas/
   30.       55 chars [linked  ] www.reuters.com                [Synthetic] Article from https://www.reuters.com/world/us/white-house-moves
   31.       55 chars [linked  ] www.politico.com               [Synthetic] Article from https://www.politico.com/news/magazine/2025/08/28/
   32.       55 chars [linked  ] taskandpurpose.com             [Synthetic] Article from https://taskandpurpose.com/history/what-is-the-dep
   33.       55 chars [linked  ] apnews.com                     [Synthetic] Article from https://apnews.com/live/donald-trump-news-updates-
   34.       55 chars [linked  ] www.bbc.com                    [Synthetic] Article from https://www.bbc.com/news/articles/cgr9r4qr0ppo
   35.       55 chars [linked  ] www.reuters.com                [Synthetic] Article from https://www.reuters.com/world/us/trump-orders-retu
   36.       55 chars [linked  ] www.usnews.com                 [Synthetic] Article from https://www.usnews.com/news/us/articles/2025-09-05
   37.       55 chars [linked  ] www.politico.com               [Synthetic] Article from https://www.politico.com/news/2025/09/05/pentagon-
   38.       55 chars [linked  ] www.washingtonpost.com         [Synthetic] Article from https://www.washingtonpost.com/national-security/2
   39.       55 chars [linked  ] www.cbsnews.com                [Synthetic] Article from https://www.cbsnews.com/news/trump-department-of-w
   40.       55 chars [linked  ] edition.cnn.com                [Synthetic] Article from https://edition.cnn.com/2025/09/09/politics/war-de
   41.       55 chars [linked  ] edition.cnn.com                [Synthetic] Article from https://edition.cnn.com/2025/09/12/politics/secret
   42.       55 chars [linked  ] www.theguardian.com            [Synthetic] Article from https://www.theguardian.com/us-news/2025/sep/11/tr
   43.       55 chars [linked  ] www.politico.com               [Synthetic] Article from https://www.politico.com/news/2025/09/17/dod-kirk-
   44.       55 chars [linked  ] www.thenation.com              [Synthetic] Article from https://www.thenation.com/article/politics/departm
   45.       55 chars [linked  ] apnews.com                     [Synthetic] Article from https://apnews.com/article/flu-shots-military-pent
   46.       55 chars [linked  ] apnews.com                     [Synthetic] Article from https://apnews.com/live/donald-trump-news-updates-
   47.       55 chars [linked  ] www.hklaw.com                  [Synthetic] Article from https://www.hklaw.com/-/media/files/insights/publi
   48.       55 chars [linked  ] www.washingtonpost.com         [Synthetic] Article from https://www.washingtonpost.com/opinions/2025/10/03
   49.       55 chars [linked  ] www.independent.co.uk          [Synthetic] Article from https://www.independent.co.uk/news/world/americas/
   50.       55 chars [linked  ] apnews.com                     [Synthetic] Article from https://apnews.com/article/university-virginia-tru
   51.       55 chars [linked  ] www.bloomberg.com              [Synthetic] Article from https://www.bloomberg.com/opinion/articles/2025-10
   52.       55 chars [linked  ] www.theatlantic.com            [Synthetic] Article from https://www.theatlantic.com/newsletters/archive/20
   53.       55 chars [linked  ] www.independent.co.uk          [Synthetic] Article from https://www.independent.co.uk/news/world/americas/
   54.       55 chars [linked  ] theconversation.com            [Synthetic] Article from https://theconversation.com/warrior-ethos-mistakes
   55.       55 chars [linked  ] www.theatlantic.com            [Synthetic] Article from https://www.theatlantic.com/national-security/arch
   56.       55 chars [linked  ] www.nytimes.com                [Synthetic] Article from https://www.nytimes.com/2025/10/08/us/politics/bla
   57.       55 chars [linked  ] edition.cnn.com                [Synthetic] Article from https://edition.cnn.com/2025/10/15/politics/hegset
   58.       55 chars [linked  ] thehill.com                    [Synthetic] Article from https://thehill.com/opinion/national-security/5554
   59.       55 chars [linked  ] www.timesofisrael.com          [Synthetic] Article from https://www.timesofisrael.com/full-text-of-trumps-
   60.       55 chars [linked  ] edition.cnn.com                [Synthetic] Article from https://edition.cnn.com/2025/09/30/politics/quanti
   61.       55 chars [linked  ] www.wsj.com                    [Synthetic] Article from https://www.wsj.com/opinion/uva-justice-department
   62.       55 chars [linked  ] thehill.com                    [Synthetic] Article from https://thehill.com/opinion/national-security/5580
   63.       55 chars [linked  ] www.politico.com               [Synthetic] Article from https://www.politico.com/news/2025/11/26/pete-hegs
   64.       55 chars [linked  ] www.nytimes.com                [Synthetic] Article from https://www.nytimes.com/2025/11/28/us/politics/nor
   65.       55 chars [linked  ] www.independent.co.uk          [Synthetic] Article from https://www.independent.co.uk/news/world/americas/
   66.       55 chars [linked  ] www.timesofisrael.com          [Synthetic] Article from https://www.timesofisrael.com/liveblog_entry/netan
   67.       55 chars [linked  ] www.theguardian.com            [Synthetic] Article from https://www.theguardian.com/us-news/2026/jan/15/de
   68.       55 chars [linked  ] www.thenation.com              [Synthetic] Article from https://www.thenation.com/article/world/trump-war-
   69.       55 chars [linked  ] www.nytimes.com                [Synthetic] Article from https://www.nytimes.com/2026/01/14/us/defense-depa
   70.       55 chars [linked  ] www.theguardian.com            [Synthetic] Article from https://www.theguardian.com/us-news/2026/feb/04/bo
   71.       55 chars [linked  ] apnews.com                     [Synthetic] Article from https://apnews.com/article/harvard-defense-departm
   72.       55 chars [linked  ] www.foxnews.com                [Synthetic] Article from https://www.foxnews.com/politics/hegseth-ending-mi
   73.       55 chars [linked  ] www.foxnews.com                [Synthetic] Article from https://www.foxnews.com/politics/hegseth-bans-mili
   74.       55 chars [linked  ] www.reuters.com                [Synthetic] Article from https://www.reuters.com/world/us/hegseth-says-pent
   75.       55 chars [linked  ] thehill.com                    [Synthetic] Article from https://thehill.com/policy/defense/5742833-hegseth
   76.       55 chars [linked  ] www.aol.com                    [Synthetic] Article from https://www.aol.com/articles/pentagon-may-bar-tuit
   77.       55 chars [linked  ] edition.cnn.com                [Synthetic] Article from https://edition.cnn.com/2025/01/25/politics/pentag
   78.       55 chars [linked  ] edition.cnn.com                [Synthetic] Article from https://edition.cnn.com/2026/02/13/politics/us-mil
   79.       55 chars [linked  ] www.bbc.co.uk                  [Synthetic] Article from https://www.bbc.co.uk/news/articles/cn0zrwzr519o
   80.       55 chars [linked  ] www.nytimes.com                [Synthetic] Article from https://www.nytimes.com/2026/02/28/us/politics/pen
   81.       55 chars [linked  ] www.theatlantic.com            [Synthetic] Article from https://www.theatlantic.com/ideas/2026/02/pete-heg
   82.       55 chars [linked  ] www.reuters.com                [Synthetic] Article from https://www.reuters.com/technology/artificial-inte
   83.       55 chars [linked  ] www.theguardian.com            [Synthetic] Article from https://www.theguardian.com/us-news/2025/jan/27/tr
   84.       55 chars [linked  ] www.bbc.com                    [Synthetic] Article from https://www.bbc.com/news/articles/czj3v42m9x0o
   85.       55 chars [linked  ] www.politico.com               [Synthetic] Article from https://www.politico.com/news/2025/01/28/army-trum
   86.       55 chars [linked  ] www.akingump.com               [Synthetic] Article from https://www.akingump.com/en/insights/blogs/trump-e
   87.       55 chars [linked  ] thehill.com                    [Synthetic] Article from https://thehill.com/homenews/administration/511018
   88.       55 chars [linked  ] www.foxnews.com                [Synthetic] Article from https://www.foxnews.com/politics/trump-dod-creates
   89.       55 chars [linked  ] apnews.com                     [Synthetic] Article from https://apnews.com/article/dei-purge-images-pentag
   90.       55 chars [linked  ] www.theguardian.com            [Synthetic] Article from https://www.theguardian.com/us-news/2025/feb/27/tr
   91.       55 chars [linked  ] www.theguardian.com            [Synthetic] Article from https://www.theguardian.com/us-news/2025/mar/07/mi
   92.       55 chars [linked  ] www.euronews.com               [Synthetic] Article from https://www.euronews.com/culture/2025/03/07/us-def
   93.       55 chars [linked  ] theweek.com                    [Synthetic] Article from https://theweek.com/politics/trump-hegseth-militar
   94.       55 chars [linked  ] www.reuters.com                [Synthetic] Article from https://www.reuters.com/world/us/trump-administrat
   95.       55 chars [linked  ] www.nytimes.com                [Synthetic] Article from https://www.nytimes.com/2025/04/14/us/politics/tru
   96.       55 chars [linked  ] www.nytimes.com                [Synthetic] Article from https://www.nytimes.com/2025/04/20/us/politics/tru
   97.       55 chars [linked  ] www.reuters.com                [Synthetic] Article from https://www.reuters.com/world/us/trump-administrat
   98.       55 chars [linked  ] www.bbc.com                    [Synthetic] Article from https://www.bbc.com/news/articles/c5yg2y21yq5o
   99.       55 chars [linked  ] www.reuters.com                [Synthetic] Article from https://www.reuters.com/world/us/pentagon-chief-ca
  100.       55 chars [linked  ] time.com                       [Synthetic] Article from https://time.com/7280106/trump-interview-100-days-
  101.       55 chars [linked  ] www.reuters.com                [Synthetic] Article from https://www.reuters.com/world/us/trump-requests-89
  102.       55 chars [linked  ] www.theweek.com                [Synthetic] Article from https://www.theweek.com/politics/military-hegseth-
  103.       55 chars [linked  ] www.reuters.com                [Synthetic] Article from https://www.reuters.com/world/us/west-point-trump-
  104.       55 chars [linked  ] www.theguardian.com            [Synthetic] Article from https://www.theguardian.com/us-news/2025/may/24/tr
  105.       55 chars [linked  ] www.ms.now                     [Synthetic] Article from https://www.ms.now/rachel-maddow-show/maddowblog/w
  106.       55 chars [linked  ] www.usatoday.com               [Synthetic] Article from https://www.usatoday.com/story/news/politics/2025/
  107.       55 chars [linked  ] www.ms.now                     [Synthetic] Article from https://www.ms.now/rachel-maddow-show/maddowblog/t
  108.       55 chars [linked  ] www.nytimes.com                [Synthetic] Article from https://www.nytimes.com/2025/07/29/us/politics/gen
  109.       55 chars [linked  ] apnews.com                     [Synthetic] Article from https://apnews.com/article/trump-generals-meeting-
  110.       55 chars [linked  ] www.foxnews.com                [Synthetic] Article from https://www.foxnews.com/politics/white-house-advan
  111.       55 chars [linked  ] www.nytimes.com                [Synthetic] Article from https://www.nytimes.com/2025/09/05/us/politics/tru
  112.       55 chars [linked  ] www.reuters.com                [Synthetic] Article from https://www.reuters.com/world/us/following-pentago
  113.       55 chars [linked  ] www.theweek.com                [Synthetic] Article from https://www.theweek.com/politics/rebrands-bringing
  114.       55 chars [linked  ] apnews.com                     [Synthetic] Article from https://apnews.com/article/trump-promises-ukraine-
  115.       55 chars [linked  ] thehill.com                    [Synthetic] Article from https://thehill.com/opinion/white-house/5504305-tr
  116.       55 chars [linked  ] blogs.lse.ac.uk                [Synthetic] Article from https://blogs.lse.ac.uk/usappblog/2025/09/18/donal
  117.       55 chars [linked  ] www.independent.co.uk          [Synthetic] Article from https://www.independent.co.uk/tv/news/trump-depart
  118.       55 chars [linked  ] www.theguardian.com            [Synthetic] Article from https://www.theguardian.com/us-news/2025/sep/21/tr
  119.       55 chars [linked  ] www.lemonde.fr                 [Synthetic] Article from https://www.lemonde.fr/en/international/article/20
  120.       55 chars [linked  ] foreignpolicy.com              [Synthetic] Article from https://foreignpolicy.com/2025/09/23/nobel-peace-p
  121.       55 chars [linked  ] www.politico.com               [Synthetic] Article from https://www.politico.com/news/2025/09/30/trump-mil
  122.       55 chars [linked  ] www.theguardian.com            [Synthetic] Article from https://www.theguardian.com/us-news/2025/sep/30/pe
  123.       55 chars [linked  ] www.aljazeera.com              [Synthetic] Article from https://www.aljazeera.com/news/2025/9/29/its-about
  124.       55 chars [linked  ] www.theguardian.com            [Synthetic] Article from https://www.theguardian.com/us-news/2025/sep/05/de
  125.       55 chars [linked  ] www.lemonde.fr                 [Synthetic] Article from https://www.lemonde.fr/en/international/article/20
  126.       55 chars [linked  ] www.politico.com               [Synthetic] Article from https://www.politico.com/news/2025/09/04/white-hou
  127.       55 chars [linked  ] www.aljazeera.com              [Synthetic] Article from https://www.aljazeera.com/news/2025/9/5/us-departm
  128.       55 chars [linked  ] www.nbcnews.com                [Synthetic] Article from https://www.nbcnews.com/politics/trump-administrat
  129.       55 chars [linked  ] www.aa.com.tr                  [Synthetic] Article from https://www.aa.com.tr/en/americas/trump-opposes-pe
  130.       55 chars [linked  ] theintercept.com               [Synthetic] Article from https://theintercept.com/2025/09/21/department-of-
  131.       55 chars [linked  ] www.theguardian.com            [Synthetic] Article from https://www.theguardian.com/us-news/2025/sep/04/tr
  132.       55 chars [linked  ] news.sky.com                   [Synthetic] Article from https://news.sky.com/story/hegseth-and-trump-decla
  133.       55 chars [linked  ] www.reuters.com                [Synthetic] Article from https://www.reuters.com/world/us/trump-attend-meet
  134.       55 chars [linked  ] www.dw.com                     [Synthetic] Article from https://www.dw.com/en/trump-tells-generals-us-face
  135.       55 chars [linked  ] www.politico.com               [Synthetic] Article from https://www.politico.com/news/2025/10/04/trump-nat
  136.       55 chars [linked  ] www.theguardian.com            [Synthetic] Article from https://www.theguardian.com/us-news/2025/oct/03/us
  137.       55 chars [linked  ] inkstickmedia.com              [Synthetic] Article from https://inkstickmedia.com/the-us-military-isnt-a-p
  138.       55 chars [linked  ] www.theguardian.com            [Synthetic] Article from https://www.theguardian.com/commentisfree/2025/oct
  139.       55 chars [linked  ] news.sky.com                   [Synthetic] Article from https://news.sky.com/story/donald-trump-authorises
  140.       55 chars [linked  ] www.theweek.com                [Synthetic] Article from https://www.theweek.com/politics/things-donald-tru
  141.       55 chars [linked  ] www.foreignaffairs.com         [Synthetic] Article from https://www.foreignaffairs.com/united-states/dilem
  142.       55 chars [linked  ] www.theatlantic.com            [Synthetic] Article from https://www.theatlantic.com/politics/archive/2025/
  143.       55 chars [linked  ] www.nbcnews.com                [Synthetic] Article from https://www.nbcnews.com/politics/national-security
  144.       55 chars [linked  ] time.com                       [Synthetic] Article from https://time.com/7329734/military-integration-dei-
  145.       55 chars [linked  ] www.theweek.com                [Synthetic] Article from https://www.theweek.com/history/how-the-war-depart
  146.       55 chars [linked  ] www.aljazeera.com              [Synthetic] Article from https://www.aljazeera.com/news/2025/12/18/does-lat
  147.       55 chars [linked  ] www.yahoo.com                  [Synthetic] Article from https://www.yahoo.com/news/articles/trump-insane-p
  148.       55 chars [linked  ] www.theguardian.com            [Synthetic] Article from https://www.theguardian.com/commentisfree/2025/nov
  149.       55 chars [linked  ] www.politico.com               [Synthetic] Article from https://www.politico.com/newsletters/national-secu
  150.       55 chars [linked  ] www.politico.com               [Synthetic] Article from https://www.politico.com/newsletters/playbook-pm/2
  151.       55 chars [linked  ] time.com                       [Synthetic] Article from https://time.com/7323373/trump-chicago-national-gu
  152.       55 chars [linked  ] www.theguardian.com            [Synthetic] Article from https://www.theguardian.com/commentisfree/2025/oct
  153.       55 chars [linked  ] www.thenation.com              [Synthetic] Article from https://www.thenation.com/article/society/trump-is
  154.       55 chars [linked  ] www.thenation.com              [Synthetic] Article from https://www.thenation.com/article/politics/donald-
  155.       55 chars [linked  ] www.lemonde.fr                 [Synthetic] Article from https://www.lemonde.fr/en/opinion/article/2025/10/
  156.       55 chars [linked  ] www.ms.now                     [Synthetic] Article from https://www.ms.now/top-stories/latest/department-w
  157.       55 chars [linked  ] foreignpolicy.com              [Synthetic] Article from https://foreignpolicy.com/2025/12/08/trump-nationa
  158.       55 chars [linked  ] www.theguardian.com            [Synthetic] Article from https://www.theguardian.com/us-news/2026/jan/05/tr
  159.       55 chars [linked  ] apnews.com                     [Synthetic] Article from https://apnews.com/article/trump-defense-war-depar
  160.       55 chars [linked  ] www.forbes.com                 [Synthetic] Article from https://www.forbes.com/sites/antoniopequenoiv/2026
  161.       55 chars [linked  ] time.com                       [Synthetic] Article from https://time.com/7346623/trump-department-of-war-d
  162.       55 chars [linked  ] www.foxnews.com                [Synthetic] Article from https://www.foxnews.com/politics/white-house-defen
  163.       55 chars [linked  ] www.reuters.com                [Synthetic] Article from https://www.reuters.com/world/us/pentagons-hegseth
  164.       55 chars [linked  ] www.reuters.com                [Synthetic] Article from https://www.reuters.com/world/us/pentagon-pull-mil
  165.       55 chars [linked  ] www.reuters.com                [Synthetic] Article from https://www.reuters.com/world/us/five-former-us-de
  166.       55 chars [linked  ] www.csmonitor.com              [Synthetic] Article from https://www.csmonitor.com/USA/Military/2025/0318/t
  167.       55 chars [linked  ] nypost.com                     [Synthetic] Article from https://nypost.com/2025/02/22/us-news/how-trump-ch
  168.       55 chars [linked  ] www.nytimes.com                [Synthetic] Article from https://www.nytimes.com/2025/03/25/us/politics/pet
  169.       55 chars [linked  ] www.akingump.com               [Synthetic] Article from https://www.akingump.com/en/insights/trump-executi
  170.       55 chars [linked  ] www.nytimes.com                [Synthetic] Article from https://www.nytimes.com/interactive/2025/03/31/hea
  171.       55 chars [linked  ] www.aljazeera.com              [Synthetic] Article from https://www.aljazeera.com/news/2025/4/11/us-senate
  172.       55 chars [linked  ] www.foxnews.com                [Synthetic] Article from https://www.foxnews.com/politics/general-helped-tr
  173.       55 chars [linked  ] theintercept.com               [Synthetic] Article from https://theintercept.com/2025/04/25/hegseth-defens
  174.       55 chars [linked  ] www.csmonitor.com              [Synthetic] Article from https://www.csmonitor.com/World/2025/0912/trump-de
  175.       55 chars [linked  ] www.bbc.com                    [Synthetic] Article from https://www.bbc.com/pidgin/articles/c805l451l4yo
  176.       55 chars [linked  ] apnews.com                     [Synthetic] Article from https://apnews.com/article/trump-dei-tuskegee-airm
  177.       55 chars [linked  ] www.lemonde.fr                 [Synthetic] Article from https://www.lemonde.fr/en/opinion/article/2025/02/
  178.       55 chars [linked  ] www.bbc.co.uk                  [Synthetic] Article from https://www.bbc.co.uk/news/articles/czj3v42m9x0o
  179.       55 chars [linked  ] www.nbcnews.com                [Synthetic] Article from https://www.nbcnews.com/politics/national-security
  180.       55 chars [linked  ] www.bbc.com                    [Synthetic] Article from https://www.bbc.com/news/articles/cp3xq9wx5zyo
  181.       55 chars [linked  ] www.csmonitor.com              [Synthetic] Article from https://www.csmonitor.com/USA/Military/2025/0924/t
  182.       55 chars [linked  ] www.bbc.com                    [Synthetic] Article from https://www.bbc.com/news/articles/cvgq044n72po
  183.       55 chars [linked  ] www.lemonde.fr                 [Synthetic] Article from https://www.lemonde.fr/en/international/article/20
  184.       55 chars [linked  ] news.sky.com                   [Synthetic] Article from https://news.sky.com/story/who-is-pete-hegseth-the
  185.       55 chars [linked  ] www.aljazeera.com              [Synthetic] Article from https://www.aljazeera.com/news/2025/12/11/despite-
  186.       55 chars [linked  ] theconversation.com            [Synthetic] Article from https://theconversation.com/topics/department-of-w
  187.       55 chars [linked  ] www.nytimes.com                [Synthetic] Article from https://www.nytimes.com/2025/12/10/opinion/trump-n
  188.       55 chars [linked  ] www.aa.com.tr                  [Synthetic] Article from https://www.aa.com.tr/en/americas/us-congressional
  189.       55 chars [linked  ] www.politico.com               [Synthetic] Article from https://www.politico.com/news/magazine/2026/01/20/
  190.       55 chars [linked  ] www.bloomberg.com              [Synthetic] Article from https://www.bloomberg.com/news/articles/2025-01-14
  191.       55 chars [linked  ] www.nytimes.com                [Synthetic] Article from https://www.nytimes.com/2025/01/15/opinion/pete-he
  192.       55 chars [linked  ] www.theatlantic.com            [Synthetic] Article from https://www.theatlantic.com/ideas/archive/2025/01/
  193.       55 chars [linked  ] www.washingtonpost.com         [Synthetic] Article from https://www.washingtonpost.com/books/2025/01/22/pe
  194.       55 chars [linked  ] edition.cnn.com                [Synthetic] Article from https://edition.cnn.com/2025/01/15/politics/pete-h
  195.       55 chars [linked  ] thehill.com                    [Synthetic] Article from https://thehill.com/homenews/education/5119424-heg
  196.       55 chars [linked  ] www.bloomberg.com              [Synthetic] Article from https://www.bloomberg.com/news/articles/2025-01-28
  197.       55 chars [linked  ] www.theguardian.com            [Synthetic] Article from https://www.theguardian.com/us-news/2025/jan/24/pe
  198.       55 chars [linked  ] theintercept.com               [Synthetic] Article from https://theintercept.com/2025/02/12/pete-hegseth-m
  199.       55 chars [linked  ] www.nbcnews.com                [Synthetic] Article from https://www.nbcnews.com/politics/congress/senate-a
  200.       55 chars [linked  ] www.newsweek.com               [Synthetic] Article from https://www.newsweek.com/who-dan-caine-three-star-
  201.       55 chars [linked  ] www.bbc.com                    [Synthetic] Article from https://www.bbc.com/news/articles/clyr2xvn4dpo
  202.       55 chars [linked  ] www.nytimes.com                [Synthetic] Article from https://www.nytimes.com/2025/02/23/us/politics/tru
  203.       55 chars [linked  ] www.washingtonpost.com         [Synthetic] Article from https://www.washingtonpost.com/national-security/2
  204.       55 chars [linked  ] www.theatlantic.com            [Synthetic] Article from https://www.theatlantic.com/ideas/archive/2025/02/
  205.       55 chars [linked  ] www.reuters.com                [Synthetic] Article from https://www.reuters.com/world/us/hes-real-general-
  206.       55 chars [linked  ] www.thehill.com                [Synthetic] Article from https://www.thehill.com/policy/defense/5159471-wha
  207.       55 chars [linked  ] www.foxnews.com                [Synthetic] Article from https://www.foxnews.com/politics/who-daniel-razin-
  208.       55 chars [linked  ] www.cbsnews.com                [Synthetic] Article from https://www.cbsnews.com/news/pentagon-hegseth-revi
  209.       55 chars [linked  ] apnews.com                     [Synthetic] Article from https://apnews.com/article/pentagon-military-budge
  210.       55 chars [linked  ] www.forbes.com                 [Synthetic] Article from https://www.forbes.com/sites/danalexander/2025/04/
  211.       55 chars [linked  ] www.theguardian.com            [Synthetic] Article from https://www.theguardian.com/us-news/2025/feb/28/de
  212.       55 chars [linked  ] www.nytimes.com                [Synthetic] Article from https://www.nytimes.com/2025/02/27/opinion/trump-h
  213.       55 chars [linked  ] www.bloomberg.com              [Synthetic] Article from https://www.bloomberg.com/news/articles/2025-03-14
  214.       55 chars [linked  ] www.nytimes.com                [Synthetic] Article from https://www.nytimes.com/2025/03/04/us/politics/heg
  215.       55 chars [linked  ] www.thehill.com                [Synthetic] Article from https://www.thehill.com/opinion/national-security/
  216.       55 chars [linked  ] www.thehill.com                [Synthetic] Article from https://www.thehill.com/opinion/national-security/
  217.       55 chars [linked  ] www.theguardian.com            [Synthetic] Article from https://www.theguardian.com/us-news/2025/mar/15/pe
  218.       55 chars [linked  ] www.theguardian.com            [Synthetic] Article from https://www.theguardian.com/us-news/2025/mar/13/pe
  219.       55 chars [linked  ] subscriber.politicopro.com     [Synthetic] Article from https://subscriber.politicopro.com/article/2025/03
  220.       55 chars [linked  ] thehill.com                    [Synthetic] Article from https://thehill.com/opinion/national-security/5178
  221.       55 chars [linked  ] www.wsj.com                    [Synthetic] Article from https://www.wsj.com/politics/national-security/whi
  222.       55 chars [linked  ] www.thehill.com                [Synthetic] Article from https://www.thehill.com/policy/defense/5469171-tru
  223.       55 chars [linked  ] www.aljazeera.com              [Synthetic] Article from https://www.aljazeera.com/news/2025/9/5/trump-symb
  224.       55 chars [linked  ] www.forbes.com                 [Synthetic] Article from https://www.forbes.com/sites/dougmelville/2025/09/
  225.       55 chars [linked  ] www.theatlantic.com            [Synthetic] Article from https://www.theatlantic.com/ideas/archive/2025/09/
  226.       55 chars [linked  ] slate.com                      [Synthetic] Article from https://slate.com/news-and-politics/2025/09/vance-
  227.       55 chars [linked  ] edition.cnn.com                [Synthetic] Article from https://edition.cnn.com/2025/09/26/politics/pentag
  228.       55 chars [linked  ] www.foxnews.com                [Synthetic] Article from https://www.foxnews.com/politics/war-department-pu
  229.       55 chars [linked  ] www.theguardian.com            [Synthetic] Article from https://www.theguardian.com/us-news/2025/sep/30/pe
  230.       55 chars [linked  ] www.washingtonpost.com         [Synthetic] Article from https://www.washingtonpost.com/national-security/2
  231.       55 chars [linked  ] www.politico.com               [Synthetic] Article from https://www.politico.com/news/2025/10/09/hegseth-c
  232.       55 chars [linked  ] www.bbc.co.uk                  [Synthetic] Article from https://www.bbc.co.uk/news/articles/cnvgv9v2vd6o
  233.       55 chars [linked  ] edition.cnn.com                [Synthetic] Article from https://edition.cnn.com/2025/09/10/politics/venezu
  234.       55 chars [linked  ] edition.cnn.com                [Synthetic] Article from https://edition.cnn.com/2025/10/11/politics/milita
  235.       55 chars [linked  ] edition.cnn.com                [Synthetic] Article from https://edition.cnn.com/2025/12/17/politics/warrio
  236.       55 chars [linked  ] edition.cnn.com                [Synthetic] Article from https://edition.cnn.com/2026/01/15/politics/depart
  237.       55 chars [linked  ] www.bbc.com                    [Synthetic] Article from https://www.bbc.com/news/live/cr53m3g32n6t
  238.       55 chars [linked  ] www.bbc.co.uk                  [Synthetic] Article from https://www.bbc.co.uk/news/articles/cg45l9q62w4o
  239.       55 chars [linked  ] www.politico.com               [Synthetic] Article from https://www.politico.com/live-updates/2025/01/20/d
  240.       55 chars [linked  ] thehill.com                    [Synthetic] Article from https://thehill.com/newsletters/defense-national-s
  241.       55 chars [linked  ] www.independent.co.uk          [Synthetic] Article from https://www.independent.co.uk/news/world/americas/
  242.       55 chars [linked  ] www.foxnews.com                [Synthetic] Article from https://www.foxnews.com/politics/scouting-america-
  243.       55 chars [linked  ] edition.cnn.com                [Synthetic] Article from https://edition.cnn.com/2025/12/04/politics/hegset
  244.       55 chars [linked  ] thehill.com                    [Synthetic] Article from https://thehill.com/policy/defense/5263698-defense
  245.       55 chars [linked  ] www.reuters.com                [Synthetic] Article from https://www.reuters.com/world/us/naacp-says-trump-
  246.       55 chars [linked  ] www.newsonair.gov.in           [Synthetic] Article from https://www.newsonair.gov.in/us-president-donald-t
  247.       55 chars [linked  ] www.foreignaffairs.com         [Synthetic] Article from https://www.foreignaffairs.com/united-states/trump
  248.       55 chars [linked  ] www.reuters.com                [Synthetic] Article from https://www.reuters.com/world/us/trump-pushes-out-
  249.       55 chars [linked  ] www.nytimes.com                [Synthetic] Article from https://www.nytimes.com/2025/02/24/opinion/trump-h
  250.       55 chars [linked  ] www.aa.com.tr                  [Synthetic] Article from https://www.aa.com.tr/en/americas/ex-us-defense-ch
  251.       55 chars [bm25_unlinked] theweek.com                    The military: Hegseth's escalating culture war
  252.       55 chars [bm25_unlinked] nypost.com                     Hegseth bans military from attending Princeton, Columbia, other elite universiti
  253.       55 chars [bm25_unlinked] www.cbsnews.com                CBS News
  254.       55 chars [bm25_unlinked] www.cbsnews.com                Pentagon to impose new limits on reporters' access, documents show
  255.       55 chars [bm25_unlinked] www.wsj.com                    Major Media Outlets Rebuff New Pentagon Press Policy
  256.       55 chars [bm25_unlinked] www.yahoo.com                  Pentagon Pete Hegseth Imposes Sweeping Restrictions on Press and Personnel
  257.       55 chars [bm25_unlinked] www.bbc.com                    Boat strike briefings help Hegseth - but shipwreck video release may hurt
  258.       55 chars [bm25_unlinked] www.independent.co.uk          Inside Pete Hegseth’s chaotic inner circle as report says defense secretary’s te
  259.       55 chars [bm25_unlinked] www.yahoo.com                  Hegseth Told To 'Come Clean' Over Bizarre Claim He Was 'Targeted' By 'Deep State
  260.       55 chars [bm25_unlinked] www.reuters.com                Hegseth supports women's right to vote, Pentagon says, despite his having repost
  261.       55 chars [bm25_unlinked] www.nytimes.com                The Pentagon’s Culture Wars Strike West Point
  262.       55 chars [bm25_unlinked] theintercept.com               Pete Hegseth Is Mad the Media Won't Celebrate U.S. War With Iran
  263.       55 chars [bm25_unlinked] www.theguardian.com            Pete Hegseth reposts video that says women shouldn’t be allowed to vote
  264.       55 chars [bm25_unlinked] edition.cnn.com                Hegseth orders prior approval for ‘all interactions’ between military officials
  265.       55 chars [bm25_unlinked] edition.cnn.com                Trump unlikely to dismiss Hegseth, but officials are troubled by disarray in Pen
  266.       55 chars [bm25_unlinked] www.nytimes.com                Hegseth Invokes Divine Purpose to Justify Military Might
  267.       55 chars [bm25_unlinked] www.reuters.com                US defense chief Hegseth leads Christian prayer service at Pentagon
  268.       55 chars [bm25_unlinked] www.aol.com                    Pentagon attacks Washington Post over report on Hegseth’s security costs
  269.       55 chars [bm25_unlinked] www.theguardian.com            Pentagon names new press corps from far-right outlets after reporter walkout
  270.       55 chars [bm25_unlinked] www.theatlantic.com            Pete Hegseth Should Stay Out of Minneapolis
  271.       55 chars [bm25_unlinked] www.theguardian.com            How Pete Hegseth’s zeal to bring religiosity to the Pentagon is dividing the mil
  272.       55 chars [bm25_unlinked] apnews.com                     How the military is dealing with Hegseth's order to remove transgender troops
  273.       55 chars [bm25_unlinked] www.theguardian.com            Pete Hegseth’s attempt to gag journalism is a resounding failure
  274.       55 chars [bm25_unlinked] www.theatlantic.com            The Enemy That Hegseth and Trump Insist on Honoring
  275.       55 chars [bm25_unlinked] www.aol.com                    Trump cracks joke at embattled Pete Hegseth’s expense in cabinet meeting: ‘My le
  276.       55 chars [bm25_unlinked] www.theguardian.com            White House stunned as Hegseth inquiry brings up illegal wiretap claims
  277.       55 chars [bm25_unlinked] www.theatlantic.com            Trump and Hegseth's Hysterical Reaction to an Ad
  278.       55 chars [bm25_unlinked] www.theatlantic.com            The Administration Wants Military Women to Know Their Place
  279.       55 chars [bm25_unlinked] www.independent.co.uk          Pete Hegseth again uses Iran war update to tear into media and assures Americans
  280.       55 chars [bm25_unlinked] www.theguardian.com            Pete Hegseth issues formal censure to Democratic senator Mark Kelly
  281.       55 chars [bm25_unlinked] www.theguardian.com            Trump news at a glance: White House doesn't trust Hegseth to choose new chief of
  282.       55 chars [bm25_unlinked] www.reuters.com                US defence chief looks to woo allies in Asian security forum debut
  283.       55 chars [bm25_unlinked] edition.cnn.com                Hegseth invokes Biblical language when speaking about war in Iran, researchers e
  284.       55 chars [bm25_unlinked] news.sky.com                   Donald Trump urged to fire US defence secretary Pete Hegseth over war plans leak
  285.       55 chars [bm25_unlinked] www.aljazeera.com              How Republicans and Democrats are reacting to the Yemen war plans leak
  286.       55 chars [bm25_unlinked] www.theguardian.com            Pete Hegseth suggests he would disobey court ruling against deploying military i
  287.       55 chars [bm25_unlinked] www.reuters.com                US deploying more defensive capabilities to Middle East, defense secretary says
  288.       55 chars [bm25_unlinked] www.politico.com               ‘America’s reputation is on the line’: Republican senators chide Hegseth over Uk
  289.       55 chars [bm25_unlinked] thehill.com                    Pentagon watchdog reveals classified leak
  290.       55 chars [bm25_unlinked] thehill.com                    Republican senators question Pete Hegseth
  291.       55 chars [bm25_unlinked] www.thenation.com              Pete Hegseth Is Unleashing Chaos at the Pentagon
  292.       55 chars [bm25_unlinked] www.aljazeera.com              US Defense Secretary Hegseth orders 20% cut in senior ranks of military
  293.       55 chars [bm25_unlinked] www.foxnews.com                White House rips alleged Pentagon leakers with 'shattered egos,' brushes off Heg
  294.       55 chars [bm25_unlinked] www.aol.com                    Pentagon Has Evidence Pete Hegseth Really Did Text Classified Info: Reports
  295.       55 chars [bm25_unlinked] www.theatlantic.com            The Pentagon Against the Think Tanks
  296.       55 chars [bm25_unlinked] www.timesofisrael.com          US defense secretary shared war plans with wife, brother in 2nd Signal chat - re
  297.       55 chars [bm25_unlinked] www.nytimes.com                White House Deletes Vance Post Referring to Armenian Genocide
  298.       55 chars [bm25_unlinked] www.bbc.com                    Europeans have made concessions to US over Greenland, JD Vance says
  299.       55 chars [bm25_unlinked] www.reuters.com                Vance announces new assistant attorney general role to combat taxpayer fraud
  300.       55 chars [bm25_unlinked] www.politico.com               Playbook: Inside JD Vance’s redistricting rescue mission - POLITICO
  301.       55 chars [bm25_unlinked] www.politico.com               The 'Enigma': Why JD Vance Befuddles the World
  302.       55 chars [bm25_unlinked] www.thetimes.com               JD Vance, the Iran War, and the 2028 MAGA Succession Battle
  303.       55 chars [bm25_unlinked] www.lemonde.fr                 One year after JD Vance's hostile speech against Europe in Munich, what remains?
  304.       55 chars [bm25_unlinked] www.morgenpost.de              Rubio trägt für Trump die schwersten Bürden – und hat einen Rivalen
  305.       55 chars [bm25_unlinked] www.aol.com                    Vance’s rock-and-a-hard-place: Backing Trump’s Iran war could cost him in 2028
  306.       55 chars [bm25_unlinked] www.wsj.com                    Iran Pitches New Ideas in Nuclear Talks—but Not the One That Matters to Trump
  307.       55 chars [bm25_unlinked] foxsanantonio.com              Error extracting metadata
  308.       55 chars [bm25_unlinked] www.thetimes.com               Untitled
  309.       55 chars [bm25_unlinked] www.threads.com                Vice President JD Vance defended the legality of the U.S. military operation in
  310.       55 chars [bm25_unlinked] www.reuters.com                Vance says he trusts Trump on Iran war, plays down differences
  311.       55 chars [bm25_unlinked] www.reuters.com                US Vice President Vance's office backtracks after statement on 'Armenian genocid
  312.       55 chars [bm25_unlinked] www.aol.com                    U-turning Vance's past comments haunt him again as he hails Trump 'for not start
  313.       55 chars [bm25_unlinked] www.bbc.co.uk                  US pitches plan to counter China's dominance of critical mineral supply
  314.       55 chars [bm25_unlinked] www.bbc.co.uk                  Vance criticises Denmark and Europe's handling of 'critical' Greenland
  315.       55 chars [bm25_unlinked] www.thetimes.com               JD Vance tells Europe to stop 'crazy overreactions' on Greenland
  316.       55 chars [bm25_unlinked] www.aa.com.tr                  US vice president rules out prolonged war in Middle East amid Iran tensions: Rep
  317.       55 chars [bm25_unlinked] www.aol.com                    What JD Vance did in Iraq, as told by the friend who served with him
  318.       55 chars [bm25_unlinked] www.thenation.com              JD Vance: A Prisoner of the Caucasus
  319.       55 chars [bm25_unlinked] www.aljazeera.com              Trump pauses military aid to Ukraine after bust-up with Zelenskyy
  320.       55 chars [bm25_unlinked] apnews.com                     In Washington, JD Vance and Pete Hegseth visit troops amid Trump crackdown
  321.       55 chars [bm25_unlinked] nypost.com                     JD Vance says 'no chance' a US attack on Iran will lead to long war with 'no end
  322.       55 chars [bm25_unlinked] www.independent.co.uk          JD Vance looks like the smallest man in America
  323.       55 chars [bm25_unlinked] www.independent.co.uk          Vance’s rock-and-a-hard-place: Backing Trump’s Iran war could cost him in 2028
  324.       55 chars [bm25_unlinked] www.aa.com.tr                  US vice president's silence draws scrutiny amid Iran operation
  325.       55 chars [bm25_unlinked] www.reuters.com                US officials, Vance urge Trump to try diplomacy before strikes on Iran, WSJ repo
  326.       55 chars [bm25_unlinked] www.nytimes.com                New Administration to Take Over in Challenging Era for the Pentagon
  327.       55 chars [bm25_unlinked] www.politico.com               Playbook PM: Vance delivers a gut punch to Europe - POLITICO
  328.       55 chars [bm25_unlinked] www.wsj.com                    Trump Stiffs Ukraine on Arms
  329.       55 chars [bm25_unlinked] www.bbc.co.uk                  Dan Driscoll: Who is Trump's 'drone guy' taking central role in Ukraine peace ta
  330.       55 chars [bm25_unlinked] www.theguardian.com            Why Marco Rubio’s ‘reassuring’ speech to Europe was nothing of the kind
  331.       55 chars [bm25_unlinked] www.trtworld.com               US vice president says Venezuela attack boosts control over energy resources
  332.       55 chars [bm25_unlinked] www.bbc.com                    Munich Security Conference: Trump’s world order hangs over Europe
  333.       55 chars [bm25_unlinked] www.bbc.com                    Trump speculates about regime change in Iran after US strikes
  334.       55 chars [bm25_unlinked] www.bbc.com                    Greenland ready to work with US on defence, says PM
  335.       55 chars [bm25_unlinked] www.washingtonpost.com         Iraq War critic, Venezuela mission defender: Vance’s foreign policy journey
  336.       55 chars [bm25_unlinked] nypost.com                     JD Vance attends dignified transfer for seventh US soldier killed in Iran war
  337.       55 chars [bm25_unlinked] www.nytimes.com                JD Vance Addresses Joe Kent's Resignation Amid Iran War Controversy
  338.       55 chars [bm25_unlinked] www.trtworld.com               Vance cautions Iran against dismissing US threats
  339.       55 chars [bm25_unlinked] www.lemonde.fr                 Trump aligns with Israel's most hardline positions in the war against the Palest
  340.       55 chars [bm25_unlinked] www.washingtonpost.com         In Iran crisis, Trump confronted limits of U.S. military power
  341.       55 chars [bm25_unlinked] nypost.com                     White House rejects Hegseth chief of staff pick who trashed Trump and Vance as '
  342.       55 chars [bm25_unlinked] www.theguardian.com            Vance and Hegseth attend dignified transfer ceremony – as it happened
  343.       55 chars [bm25_unlinked] www.bloomberg.com              Hegseth on Verge of Confirmation With Vance to Break Senate Tie
  344.       55 chars [bm25_unlinked] www.aa.com.tr                  US Senate blocks bill to curb Trump's war powers on Venezuela
  345.       55 chars [bm25_unlinked] apnews.com                     Presidents vs. Congress: Trump is only the latest to test the War Powers Act
  346.       55 chars [bm25_unlinked] www.cbsnews.com                Rep. Jonathan Jackson asks Defense Secretary Pete Hegseth to step down after gro
  347.       55 chars [bm25_unlinked] nypost.com                     JD Vance finally explains his whereabouts during US raid to capture Venezuela de
  348.       55 chars [bm25_unlinked] www.nytimes.com                Three American Speeches at Munich, and Plenty of Confusion
  349.       55 chars [bm25_unlinked] taskandpurpose.com             Are the U.S. and Ukraine friends again? It’s complicated.
  350.       55 chars [bm25_unlinked] thehill.com                    Vance Defends Iran Strikes
  351.       55 chars [bm25_unlinked] www.jpost.com                  Iran will not accept US President Donald Trump's 'red lines' after Geneva talks,
  352.       55 chars [bm25_unlinked] www.timesofisrael.com          Top US general says Maduro and his wife 'gave up' to American forces
  353.       55 chars [bm25_unlinked] www.theatlantic.com            The Humiliation of J. D. Vance
  354.       55 chars [bm25_unlinked] www.nytimes.com                On Iran, Vance Balances Between Trump and the Anti-Intervention Movement
  355.       55 chars [bm25_unlinked] www.politico.com               Vance's Silence on Trump's Iran War
  356.       55 chars [bm25_unlinked] thehill.com                    Vice President Vance Casts Tie-Breaking Vote to Defeat War Powers Resolution
  357.       55 chars [bm25_unlinked] www.foxnews.com                JD Vance spearheads 'war on fraud,' promises to root out taxpayer money stolen b
  358.       55 chars [bm25_unlinked] thehill.com                    Will JD Vance's Support for Iran War Put His Political Future In Question? | TRE
  359.       55 chars [bm25_unlinked] evnreport.com                  Strategic Partnerships in Motion: J.D. Vance in Armenia and Azerbaijan
  360.       55 chars [bm25_unlinked] www.washingtonpost.com         Trump weighs Venezuela strikes as U.S. forces prepare for attack order
  361.       55 chars [bm25_unlinked] caspianpost.com                Former CIA Officer: “Vance Visit Aimed at Countering Russia and Iran” - INTERVIE
  362.       55 chars [bm25_unlinked] www.aa.com.tr                  Vice President Vance says US will ensure Iran does not obtain nuclear weapon
  363.       55 chars [bm25_unlinked] www.politico.com               Vance was 'skeptical' voice in White House on Iran strikes
  364.       55 chars [bm25_unlinked] www.nytimes.com                Vance Justifies Capture as Bringing Maduro to Justice
  365.       55 chars [bm25_unlinked] edition.cnn.com                Vance’s distance from the Iran war is getting more conspicuous
  366.       55 chars [bm25_unlinked] www.thetimes.com               J.D. Vance's silence on Venezuela: MAGA, 2028
  367.       55 chars [bm25_unlinked] spectator.com                  Why is Vance silent on Iran?
  368.       55 chars [bm25_unlinked] thehill.com                    Trump Discusses Latest Developments Coming Out Of Iran
  369.       55 chars [bm25_unlinked] www.axios.com                  Trump's top general warns of Iran strike risks
  370.       55 chars [bm25_unlinked] edition.cnn.com                Greene says Iran war hurts Vance's chance of succeeding Trump
  371.       55 chars [bm25_unlinked] edition.cnn.com                Senate GOP shields Trump from limits to his war powers in Venezuela after Vance
  372.       55 chars [bm25_unlinked] edition.cnn.com                Bruising on Trump’s left hand sparks renewed questions about his health
  373.       55 chars [bm25_unlinked] www.aol.com                    Trump says he 'aced' third cognitive exam in defense of 'perfect' health
  374.       55 chars [bm25_unlinked] subscriber.politicopro.com     New York's top court weighs plan to reschedule local elections
  375.       55 chars [bm25_unlinked] apnews.com                     Iga Swiatek wins at US Open, then talks Taylor and Travis
  376.       55 chars [bm25_unlinked] www.theguardian.com            This ‘natural’ infertility treatment is backed by Maha. Is it the right’s path t
  377.       55 chars [bm25_unlinked] www.aol.com                    Trump Keeps Making This Dangerous Threat — And Experts Say It's Becoming More Cr
  378.       55 chars [bm25_unlinked] www.aol.com                    Pranksters Troll Trump Supporters By Exposing MAGA Hypocrisy Right To Their Face
  379.       55 chars [bm25_unlinked] apnews.com                     Celebrity birthdays for the week of Feb. 16-22
  380.       55 chars [bm25_unlinked] www.nytimes.com                Republicans and Democrats Push for Release of More Epstein Files
  381.       55 chars [bm25_unlinked] www.nytimes.com                Trump Transition Updates: Ethics Report Accuses Gaetz of Paying for Sex, Includi
  382.       55 chars [bm25_unlinked] www.threads.com                He's an embarrassment
  383.       55 chars [bm25_unlinked] www.abc.net.au                 AFL retracts claim that umpires gave league 'wrong information' in Lachie Schult
  384.       55 chars [bm25_unlinked] www.nytimes.com                Did That Bald Head Get Your Attention? One Start-Up Hopes So.
  385.       55 chars [bm25_unlinked] spectator.com                  'There’s an awful lot more bile now': Jonathan Lynn on how politics has changed
  386.       55 chars [bm25_unlinked] sports.yahoo.com               2026 VA Showcase Boy's PressConference
  387.       55 chars [bm25_unlinked] www.politico.com               Jolly’s first big test  - POLITICO
  388.       55 chars [bm25_unlinked] www.pulteney.sa.edu.au         Prosper Vol. 2 2024
  389.       55 chars [bm25_unlinked] thehill.com                    Trump refutes reports that top Gen. Dan Caine warned of Iran military action ris
  390.       55 chars [bm25_unlinked] www.timesofisrael.com          Top US general says report on Iran nuclear violations 'is certainly troubling'
  391.       55 chars [bm25_unlinked] www.bbc.com                    Trump hits out at reports that top US general warned against attacking Iran
  392.       55 chars [bm25_unlinked] newrepublic.com                Trump Officials Go on Frenzied Media Tour to Try to Stop War With Iran
  393.       55 chars [bm25_unlinked] edition.cnn.com                Exclusive: Top lawyer for military joint chiefs told chairman that officers shou
  394.       55 chars [bm25_unlinked] thehill.com                    WATCH LIVE: Pete Hegseth, Dan Caine discuss Iran military operation
  395.       55 chars [bm25_unlinked] www.foxnews.com                US military striking 'Iranian aligned militia groups' in Iraq, as fallen service
  396.       55 chars [bm25_unlinked] www.nytimes.com                Joint Chiefs Chairman Issues Rare Invitation to Foreign Military Leaders
  397.       55 chars [bm25_unlinked] www.washingtonpost.com         Gen. Caine, Hegseth to fly in fighter jets during Nevada demonstration
  398.       55 chars [bm25_unlinked] www.washingtonpost.com         Pentagon plan calls for major power shifts within U.S. military
  399.       55 chars [bm25_unlinked] www.bloomberg.com              Trump's New Joint Chiefs Chairman Dan Caine Has a Constitutional Oath to Keep
  400.       55 chars [bm25_unlinked] thehill.com                    Pentagon removes Joint Staff director after less than 90 days on the job
  401.       55 chars [bm25_unlinked] www.nbcnews.com                Air Force Gen. Caine details U.S. strikes on Iran
  402.       55 chars [bm25_unlinked] www.aljazeera.com              Trump hits out at reports that top general flagged risks of attacking Iran
  403.       55 chars [bm25_unlinked] govciomedia.com                Future Joint Force Will Rely on Industry-Led Tech, Joint Chiefs Chairman Says
  404.       55 chars [bm25_unlinked] www.foxnews.com                Western Hemisphere defense chiefs convene after border drone scare prompts airsp
  405.       55 chars [bm25_unlinked] www.politico.com               MAGA hat drama could taint Trump’s top military adviser pick
  406.       55 chars [bm25_unlinked] americanpolitics.fandom.com    Dan Caine
  407.       55 chars [bm25_unlinked] www.aol.com                    Balancing act: Top general tries to avoid conflict with Trump while preparing fo
  408.       55 chars [bm25_unlinked] thehill.com                    What to know about Dan Caine, Trump's pick to lead Joint Chiefs of Staff
  409.       55 chars [bm25_unlinked] www.nytimes.com                U.S. Brings Military Chiefs to Summit, Led by Gen. Caine
  410.       55 chars [bm25_unlinked] www.bbc.co.uk                  Hegseth says US strikes on Iran were 'historically successful' after ayatollah d
  411.       55 chars [bm25_unlinked] edition.cnn.com                Trump briefed this week on options for military operations in Venezuela
  412.       55 chars [bm25_unlinked] www.reuters.com                Top US, Israeli generals meet at Pentagon amid soaring Iran tensions
  413.       55 chars [bm25_unlinked] www.forever-wars.com           Dan Caine Suddenly Realizes He's The Iran War's Fall Guy
  414.       55 chars [bm25_unlinked] www.aljazeera.com              US military officials say Iran's facilities are 'destroyed' after strike
  415.       55 chars [bm25_unlinked] www.timesofisrael.com          Rubio says truce mediators sharing info on threats; floats UN backing for Gaza p
  416.       55 chars [bm25_unlinked] taskandpurpose.com             Navy SEAL Team 6 operator will be the military's new top enlisted leader
  417.       55 chars [bm25_unlinked] www.nytimes.com                General Caine Faces His First Big Test Under Trump
  418.       55 chars [bm25_unlinked] www.reuters.com                Top US general, Hegseth monitoring Iran strikes from Trump resort, person famili
  419.       55 chars [bm25_unlinked] edition.cnn.com                Trump’s top general just undercut his ‘invasion’ claims
  420.       55 chars [bm25_unlinked] www.timesofisrael.com          Trump: Top general doesn't oppose Iran war, thinks it would be 'easily won'
  421.       55 chars [bm25_unlinked] slate.com                      Why This Iran Comment From Trump’s Top General Is a Big Deal
  422.       55 chars [bm25_unlinked] www.nytimes.com                In Remarks on Iran, Hegseth Conveys Messages to Two Different Audiences
  423.       55 chars [bm25_unlinked] www.wsj.com                    Hegseth and Caine to Address Reporters on Monday
  424.       55 chars [bm25_unlinked] www.wsj.com                    Exclusive | General Dan ‘Razin’ Caine Vaults Into the Top Tier of Trump Advisers
  425.       55 chars [bm25_unlinked] voyagertechnologies.com        Voyager Adds Intelligence Leader Dan Caine to Defense & National Security Adviso
  426.       55 chars [bm25_unlinked] www.nytimes.com                Senate Confirms New Chairman of Joint Chiefs of Staff
  427.       55 chars [bm25_unlinked] www.washingtonpost.com         Trump’s top general foresees acute risks in an attack on Iran
  428.       55 chars [bm25_unlinked] www.nytimes.com                Dan Caine, Trump's Joint Chiefs Pick, Had Unusual Path to Top Ranks
  429.       55 chars [bm25_unlinked] www.nytimes.com                Trump Says Top General Predicts Easy Victory Over Iran; He Says Otherwise in Pri
  430.       55 chars [bm25_unlinked] www.nytimes.com                New Joint Chiefs Chairman Picks a Friendly Crowd for His...
  431.       55 chars [bm25_unlinked] www.timesofisrael.com          Top US general landed in Israel last night, meets Zamir, flies over Gaza
  432.       55 chars [bm25_unlinked] edition.cnn.com                Pentagon reveals new details about US strikes against Iran but leaves key questi
  433.       55 chars [bm25_unlinked] www.afp.mil.ph                 General Brawner, U.S. General Caine Highlight Enduring PH-U.S. Defense Alliance
  434.       55 chars [bm25_unlinked] www.aol.com                    Three-star general pushed out after months of 'sustained tensions' with Hegseth,
  435.       55 chars [bm25_unlinked] www.foxnews.com                Top military leaders head to Puerto Rico to thank troops supporting Caribbean mi
  436.       55 chars [bm25_unlinked] edition.cnn.com                'It takes money to kill bad guys,' Hegseth says of $200B Iran war request to Con
  437.       55 chars [bm25_unlinked] www.newsweek.com               Who Is General Dan Caine? Top US Officer Facing Boat Strike Questions
  438.       55 chars [bm25_unlinked] apnews.com                     Hegseth and Caine visit Puerto Rico as US steps up military operations in the Ca
  439.       55 chars [bm25_unlinked] www.bloomberg.com              Top US General Meets Counterpart in Seoul Amid Budget Pressure
  440.       55 chars [bm25_unlinked] www.thecipherbrief.com         Getting to Know the Next Chairman of the Joint Chiefs of Staff
  441.       55 chars [bm25_unlinked] www.nytimes.com                Top U.S. Military Officer to Visit Caribbean as Trump Pressures Venezuela
  442.       55 chars [bm25_unlinked] www.wsj.com                    Pentagon Flags Risks of a Major Operation Against Iran
  443.       55 chars [bm25_unlinked] thehill.com                    Trump administration to give briefing on Iran to Congress on Tuesday
  444.       55 chars [bm25_unlinked] thehill.com                    Army 3-star general retires early from Pentagon role as shake-ups continue
  445.       55 chars [bm25_unlinked] www.theatlantic.com            Trump's New Favorite General
  446.       55 chars [bm25_unlinked] subscriber.politicopro.com     Venezuela strike marks a turning point for US cyber warfare
  447.       55 chars [bm25_unlinked] theconversation.com            Chairman of the Joint Chiefs advises the president on use of America’s military
  448.       55 chars [bm25_unlinked] www.politico.com               Top Pentagon brass to brief lawmakers on deadly boat strike - Live Updates
  449.       55 chars [bm25_unlinked] apnews.com                     Top US military adviser visits Caribbean as Trump ramps up pressure on Venezuela
  450.       55 chars [bm25_unlinked] thehill.com                    Trump's Joint Chiefs chair says Putin won't stop at Ukraine
  451.       55 chars [bm25_unlinked] taskandpurpose.com             Gen. Caine told wingman on 9/11: 'Don't shoot anybody. I'll make the decision.'
  452.       55 chars [bm25_unlinked] www.cbsnews.com                Lt. Gen. Dan "Razin" Caine confirmed by Senate as Joint Chiefs of Staff chairman
  453.       55 chars [bm25_unlinked] thehill.com                    Top US military adviser visits Caribbean as Trump ramps up pressure on Venezuela
  454.       55 chars [bm25_unlinked] www.timesofisrael.com          Rubio, top US officials to make case for Iran attacks to Congress Tuesday
  455.       55 chars [bm25_unlinked] edition.cnn.com                Hegseth and Caine play dueling roles as key architects of Iran strike
  456.       55 chars [bm25_unlinked] www.cbsnews.com                Top officials present Trump with military options for Venezuela in the coming da
  457.       55 chars [bm25_unlinked] www.aa.com.tr                  Trump's threats of military action in Nigeria prompt closed-door talks
  458.       55 chars [bm25_unlinked] edition.cnn.com                Balancing act: Top general tries to avoid conflict with Trump while preparing fo
  459.       55 chars [bm25_unlinked] theweek.com                    Defense: Why is Trump purging the Pentagon?
  460.       55 chars [bm25_unlinked] www.reuters.com                US Senate confirms Trump pick Caine as top US military officer
  461.       55 chars [bm25_unlinked] news.sky.com                   Trump fires top US military officers - including America's most senior commander
  462.       55 chars [bm25_unlinked] edition.cnn.com                Exclusive: Three-star general pushed out amid tensions with Hegseth
  463.       55 chars [bm25_unlinked] www.reuters.com                Trump's top national security officials to brief 'Gang of Eight' US lawmakers, s
  464.       55 chars [bm25_unlinked] apnews.com                     Senate confirms Trump nominee Caine for chairman of Joint Chiefs of Staff in ove
  465.       55 chars [bm25_unlinked] edition.cnn.com                Trump holds Venezuela Oval Office meeting amid growing questions about his milit
  466.       55 chars [bm25_unlinked] www.reuters.com                Pentagon leak probe expands as two more officials put on leave
  467.       55 chars [bm25_unlinked] www.trtworld.com               Pentagon seeks over $200 billion in emergency funding for Iran war
  468.       55 chars [bm25_unlinked] www.nytimes.com                Trump Wants Europe to Defend Itself. Here's What It Would Take.
  469.       55 chars [bm25_unlinked] www.wsj.com                    Pentagon Pushes to Double Missile Production for Potential China Conflict
  470.       55 chars [bm25_unlinked] www.bloomberg.com              Feinberg Brings Wall Street Drive to Rare-Earth Minerals Push
  471.       55 chars [bm25_unlinked] www.bloomberg.com              Palantir AI system wins key Pentagon status, Reuters reports
  472.       55 chars [bm25_unlinked] www.threads.com                Deputy U.S. Defense Secretary Steve Feinberg says the Pentagon will adopt Palant
  473.       55 chars [bm25_unlinked] govciomedia.com                War Department Expands Pay Tools to Keep High-Skill Civilians
  474.       55 chars [bm25_unlinked] www.usnews.com                 Pentagon to Get First Official Briefing on Golden Dome Missile Shield Architectu
  475.       55 chars [bm25_unlinked] seekingalpha.com               Pentagon said to have named Alibaba, Baidu, and BYD for inclusion in U.S. CMC li
  476.       55 chars [bm25_unlinked] www.bloomberg.com              Pentagon Cited Alibaba on China Military Aid in Oct. 7 Memo
  477.       55 chars [bm25_unlinked] www.bloomberg.com              Billionaire Feinberg Says He'll Bring Investor Savvy to Pentagon
  478.       55 chars [bm25_unlinked] slate.com                      Trump Has Promised to Lower Prices. It Will be Impossible Without Private Equity
  479.       55 chars [bm25_unlinked] www.aol.com                    What happens inside a command center during a military strike?
  480.       55 chars [bm25_unlinked] nypost.com                     Third Pentagon official put on leave amid leak investigation
  481.       55 chars [bm25_unlinked] www.politico.com               Defense deputy secretary pick avoids saying Russia invaded Ukraine
  482.       55 chars [bm25_unlinked] www.independent.co.uk          White House downplays expectations for Trump-Putin meeting, calling it just a st
  483.       55 chars [bm25_unlinked] subscriber.politicopro.com     Pentagon seeks to limit interactions with Congress
  484.       55 chars [bm25_unlinked] thehill.com                    Trump's No. 2 Pentagon pick won't say if Russia invaded Ukraine
  485.       55 chars [bm25_unlinked] www.reuters.com                Pentagon tightens control on staff communication with US Congress
  486.       55 chars [bm25_unlinked] apnews.com                     Hegseth changes policy on how Pentagon officials communicate with Congress
  487.       55 chars [bm25_unlinked] www.independent.co.uk          Hegseth keeps target on Navy man over lethal 'narco boat' strikes with eight-wor
  488.       55 chars [bm25_unlinked] www.independent.co.uk          ‘No deal’: Putin met a tougher Trump in Alaska than the one he steamrolled in He
  489.       55 chars [bm25_unlinked] www.iss.europa.eu              Low trust: navigating transatlantic relations under Trump 2.0
  490.       55 chars [bm25_unlinked] www.nytimes.com                As Ousters Continue, F.B.I. Singles Out Employee Over Friendship With Trump Crit
  491.       55 chars [bm25_unlinked] www.washingtonpost.com         Hegseth restores, downgrades Pentagon Office of Net Assessment
  492.       55 chars [bm25_unlinked] www.creativeprocess.info       LOVE in a F*cked-Up World: DEAN SPADE on How to Build Relationships, Hook Up & R
  493.       55 chars [bm25_unlinked] www.threads.com                Marvel Mondays #4 First looks at new MCU Archives Iron Man 2 and Captain America
  494.       55 chars [bm25_unlinked] www.imdb.com                   What's Leaving Netflix in February 2026
  495.       55 chars [bm25_unlinked] www.rocksbackpages.com         Festivals and Events articles, interviews and reviews from Rock's Backpages
  496.       55 chars [bm25_unlinked] substack.com                   Most Anticipated Reads of 2025 - Anna Rose
  497.       55 chars [bm25_unlinked] yayin.taa.gov.tr               Law and Justice Review, Issue 29
  498.       55 chars [bm25_unlinked] issuu.com                      Miami, Tina, January 2025
  499.       55 chars [bm25_unlinked] issuu.com                      Doorways Fall 2024
  500.       55 chars [bm25_unlinked] janglepophub.home.blog         Discovering Good Flying Birds: Indie-Pop That Captivates (Talulahs Tape / Album
  501.       55 chars [bm25_unlinked] issuu.com                      CSO POPS Fanfare Cincinnati - Nov/Dec 2024
  502.       55 chars [bm25_unlinked] www.hollywoodreporter.com      BAFTA Film Awards 2025: ‘Emilia Pérez’ and ‘Conclave’ Lead Longlists
  503.       55 chars [bm25_unlinked] jhwikicollection-20.fandom.com Finding Dory/Credits
  504.       55 chars [bm25_unlinked] www.hollywoodreporter.com      Timothée Chalamet Jokes About Competing With Jake Gyllenhaal for Denis Villeneuv
  505.       55 chars [bm25_unlinked] www.hollywoodreporter.com      ‘Dune: Part Two’ and ‘Deadpool & Wolverine’ Lead MPSE Golden Reel Awards Feature
  506.       55 chars [bm25_unlinked] www.nytimes.com                Trump Says Broadcasters Risk Losing Licenses When Hosts Criticize Him
  507.       55 chars [bm25_unlinked] www.aol.com                    Pentagon to impose new limits on media, documents show
  508.       55 chars [bm25_unlinked] www.forever-wars.com           ICE Launches All-Out Assault on DC Businesses in Search of TV Moments for Trump
  509.       55 chars [bm25_unlinked] www.reuters.com                Trump’s first 100 days sparks global messaging battle: Peter Apps
  510.       55 chars [bm25_unlinked] www.independent.co.uk          Stephen Miller claims Trump ‘has literally saved America’ as White House continu
  511.       55 chars [bm25_unlinked] www.independent.co.uk          Trump says Russia has made ‘pretty big concession’ to peace by not seizing whole
  512.       55 chars [bm25_unlinked] www.forever-wars.com           Migrant Torture at Guantanamo Authorized by Trump Administration Memo
  513.       55 chars [bm25_unlinked] www.politico.com               Trump’s telling silence on Gaza aid worker killing
  514.       55 chars [bm25_unlinked] americanpolitics.fandom.com    Vice Chairman of the Joint Chiefs of Staff
  515.       55 chars [bm25_unlinked] subscriber.politicopro.com     Senate confirms military's next No. 2 officer
  516.       55 chars [bm25_unlinked] subscriber.politicopro.com     Top state court weighs New York City noncitizen voting law
  517.       55 chars [bm25_unlinked] subscriber.politicopro.com     Trump taps new picks after controversial Pentagon purges
  518.       55 chars [bm25_unlinked] www.aljazeera.com              Trump has made US militarism worse
  519.       55 chars [bm25_unlinked] www.businesswire.com           Tanium Appoints Adam Ostopowich to Lead Canadian Operations Under a Unified Nati
  520.       55 chars [bm25_unlinked] subscriber.politicopro.com     Bellwether suburban county's red shift is now at the center of both parties' fut
  521.       55 chars [bm25_unlinked] www.foreignaffairs.com         How U.S. Forces Should Leave Europe: And Why Trump Should Start the Process Now
  522.       55 chars [bm25_unlinked] subscriber.politicopro.com     Hochul's move to reimagine lieutenant governor races draws mixed reviews
  523.       55 chars [bm25_unlinked] www.theguardian.com            Donald Trump is losing control of American foreign policy
  524.       55 chars [bm25_unlinked] subscriber.politicopro.com     Top New York Democrats are staying out of redistricting legal fight
  525.       55 chars [bm25_unlinked] www.nbcnews.com                Pentagon considering proposal to cut thousands of troops from Europe, officials
  526.       55 chars [bm25_unlinked] apnews.com                     Brunson, reserves spark Christmas comeback as Knicks edge Cavs 126-124
  527.       55 chars [bm25_unlinked] www.politico.com               Inside the clashes between Trump and Gabbard
  528.       55 chars [bm25_unlinked] www.forever-wars.com           A Message to Alligator Alcatraz Detainees from A Guantánamo Survivor
  529.       55 chars [bm25_unlinked] www.forever-wars.com           Lists
  530.       55 chars [bm25_unlinked] www.nytimes.com                For Trump, Charlie Kirk Is a Deeply Personal Loss
  531.       55 chars [bm25_unlinked] www.abc.net.au                 FBI director Kash Patel to face congressional hearing amid missteps on Charlie K
  532.       55 chars [bm25_unlinked] www.bbc.com                    Fact-checking Donald Trump's Oval Office confrontation with Cyril Ramaphosa
  533.       55 chars [bm25_unlinked] www.forever-wars.com           The War on Terror in Minnesota (ft. Jordan Uhl & The Intercept)
  534.       55 chars [bm25_unlinked] www.nytimes.com                Newly Released Records Detail Chaos During Fatal ICE Shooting in Minneapolis
  535.       55 chars [bm25_unlinked] thehill.com                    Trump Wins the Presidency
  536.       55 chars [bm25_unlinked] www.nytimes.com                Opinionated Lists for an Overwhelming Year
  537.       55 chars [bm25_unlinked] www.nytimes.com                Zelensky Stresses Sovereignty as U.S. Pursues Peace Talks With Russia
  538.       55 chars [bm25_unlinked] www.theguardian.com            Trump signs executive order on water pressure to 'restore shower freedom'
  539.       55 chars [bm25_unlinked] www.aol.com                    Trump threatens protesters who rain on his military parade Saturday ‘will be met
  540.       55 chars [bm25_unlinked] www.wsj.com                    Macron Warns Europe and Asia Against Being Dragged Into U.S.-China Rivalry
  541.       55 chars [bm25_unlinked] www.usnews.com                 All the President’s Billionaires: The Extraordinary Wealth in Trump’s Administra
  542.       55 chars [bm25_unlinked] www.independent.co.uk          How Trump could subvert the Constitution and stay in office for a third term
  543.       55 chars [bm25_unlinked] www.foxnews.com                Hegseth orders Pentagon to make plans for major budget cuts to align with Trump
  544.       55 chars [bm25_unlinked] www.theatlantic.com            Trump Is Facing a Catastrophic Defeat in Ukraine
  545.       55 chars [bm25_unlinked] www.theguardian.com            Trump’s Washington DC takeover is straight out of a fascist playbook
  546.       55 chars [bm25_unlinked] www.theguardian.com            Killing of survivors sparks outrage – but entire US 'drug boat' war is legally s
  547.       55 chars [bm25_unlinked] www.theguardian.com            Trump administration pulls US out of body investigating Ukraine invasion
  548.       55 chars [bm25_unlinked] www.reuters.com                Second Oval Office ambush by Trump could make foreign leaders think twice
  549.       55 chars [bm25_unlinked] edition.cnn.com                Pentagon boots CNN and the Washington Post from workspace in favor of smaller co
       --------
         36,507 chars TOTAL (9,126 est. tokens)

[LLM Analysis]
  → Prompt: 1,879,695 characters, 549 media articles
  → Model: gpt-5.4
  → Estimated input: ~469,923 tokens
  → Estimated input cost: ~$1.1748 (rate: $2.5/1M)
  → DRY RUN mode: generating synthetic response
  → Loaded real phase 4 markdown: /mnt/workspace_2/bboa/documents/clustering/phase4_culture_and_media_20260328_183618.md (325,916 chars)
  ⚠ Skipping file write: input and output tokens are both 0
  ✓ Cluster fidelity validated: 5 doc clusters = 5 parsed clusters
  ✓ 325 total article-cluster assignments
    cluster_A: 74 URLs
    cluster_B: 64 URLs
    cluster_C: 85 URLs
    cluster_D: 89 URLs
    cluster_E: 13 URLs

======================================================================
BM25 Cluster Supplementation [culture_and_media]
======================================================================
  cluster_A: 90 existing + 16 new = 90 total
    + Trump taps new picks after controversial Pentagon purges
    + New York Senate and Assembly announce new leadership roles
    + What happens inside a command center during a military strike?
    ... +13 more
  cluster_B: 71 existing + 7 new = 71 total
    + Trump Calls Somalis ‘Garbage’ in Anti-Immigrant Tirade
    + Trump Appoints Michael Flynn, Walt Nauta and Other Allies to Military
    + Hegseth says Chinese nationals no longer service Pentagon cloud
    ... +4 more
  cluster_C: 89 existing + 4 new = 89 total
    + Changing Defense Department's name to Department of War could cost up
    + How the War Department became the Department of Defense – and back aga
    + Vance’s rock-and-a-hard-place: Backing Trump’s Iran war could cost him
    ... +1 more
  cluster_D: 100 existing + 11 new = 100 total
    + Trump domestic military set worse
    + Pentagon plan calls for major power shifts within U.S. military
    + Signal war plans chat: Five takeaways from leaked US top military meet
    ... +8 more
  cluster_E: 30 existing + 17 new = 30 total
    + 'Tearing down': What drives Trump's foreign policy?
    + Donald Trump is losing control of American foreign policy
    + Iraq War critic, Venezuela mission defender: Vance’s foreign policy jo
    ... +14 more

  → 55 total articles supplemented across 5 clusters
  → 344 unique articles in cluster result

======================================================================
URL LINKAGE REPORT
======================================================================

Category: culture_and_media
  Phase 1+2 → primary URLs (workflow):    409
  Phase 1+2 → media URLs (BBAO):          252
  Phase 3   → evidence bundle URLs:       252
  Phase 4   → cluster analysis URLs:      344
  Phase 5   → persisted URLs (JSONL):       0
  Phase 6   → Kaliya handoff URLs:          0
  Phase 7   → conclusion cited URLs:        0
  ⚠ URL CONTINUITY: PARTIAL — 4 URLs lost after Phase 3

======================================================================
ALL PHASES COMPLETE
======================================================================

Demo finished. Outputs in: /tmp/demo_media_linkage_9srysbiv
