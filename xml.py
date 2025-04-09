import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node geologicalsurvey
geologicalsurvey_node1744187438380 = glueContext.create_dynamic_frame.from_catalog(
    database="database_test", 
    table_name="xml_crawl_xml_files", 
    transformation_ctx="geologicalsurvey_node1744187438380")

# Script generated for node ApplyMapping
ApplyMapping_node1744188425972 = ApplyMapping.apply(
    frame=geologicalsurvey_node1744187438380, 
    mappings=[("dataqual.attracc.attraccr", "string", "dataqual.attracc.attraccr", "string"), 
              ("dataqual.complete", "string", "dataqual.complete", "string"), 
              ("dataqual.lineage.procstep.procdate", "int", "dataqual.lineage.procstep.procdate", "int"), 
              ("dataqual.lineage.procstep.procdesc", "string", "dataqual.lineage.procstep.procdesc", "string"), 
              ("dataqual.logic", "string", "dataqual.logic", "string"), 
              ("dataqual.posacc.horizpa.horizpar", "string", "dataqual.posacc.horizpa.horizpar", "string"), 
              ("dataqual.posacc.vertacc.vertaccr", "string", "dataqual.posacc.vertacc.vertaccr", "string"), 
              ("distinfo.distliab", "string", "distinfo.distliab", "string"), 
              ("distinfo.distrib.cntinfo.cntaddr.address", "string", "distinfo.distrib.cntinfo.cntaddr.address", "string"), 
              ("distinfo.distrib.cntinfo.cntaddr.addrtype", "string", "distinfo.distrib.cntinfo.cntaddr.addrtype", "string"), 
              ("distinfo.distrib.cntinfo.cntaddr.city", "string", "distinfo.distrib.cntinfo.cntaddr.city", "string"), 
              ("distinfo.distrib.cntinfo.cntaddr.postal", "int", "distinfo.distrib.cntinfo.cntaddr.postal", "int"), 
              ("distinfo.distrib.cntinfo.cntaddr.state", "string", "distinfo.distrib.cntinfo.cntaddr.state", "string"), 
              ("distinfo.distrib.cntinfo.cntemail", "string", "distinfo.distrib.cntinfo.cntemail", "string"), 
              ("distinfo.distrib.cntinfo.cntinst", "string", "distinfo.distrib.cntinfo.cntinst", "string"), 
              ("distinfo.distrib.cntinfo.cntorgp.cntorg", "string", "distinfo.distrib.cntinfo.cntorgp.cntorg", "string"), 
              ("distinfo.distrib.cntinfo.cntvoice", "string", "distinfo.distrib.cntinfo.cntvoice", "string"), 
              ("distinfo.distrib.cntinfo.hours", "string", "distinfo.distrib.cntinfo.hours", "string"), 
              ("distinfo.resdesc", "string", "distinfo.resdesc", "string"), 
              ("distinfo.stdorder", "array", "distinfo.stdorder", "array"), 
              ("eainfo.overview.eadetcit", "string", "eainfo.overview.eadetcit", "string"), 
              ("eainfo.overview.eaover", "string", "eainfo.overview.eaover", "string"), 
              ("idinfo.accconst", "string", "idinfo.accconst", "string"), 
              ("idinfo.browse.browsed", "string", "idinfo.browse.browsed", "string"), 
              ("idinfo.browse.browsen", "string", "idinfo.browse.browsen", "string"), 
              ("idinfo.browse.browset", "string", "idinfo.browse.browset", "string"), 
              ("idinfo.citation.citeinfo.geoform", "string", "idinfo.citation.citeinfo.geoform", "string"), 
              ("idinfo.citation.citeinfo.onlink", "string", "idinfo.citation.citeinfo.onlink", "string"), 
              ("idinfo.citation.citeinfo.origin", "string", "idinfo.citation.citeinfo.origin", "string"), 
              ("idinfo.citation.citeinfo.pubdate", "int", "idinfo.citation.citeinfo.pubdate", "int"), 
              ("idinfo.citation.citeinfo.pubinfo.publish", "string", "idinfo.citation.citeinfo.pubinfo.publish", "string"), 
              ("idinfo.citation.citeinfo.pubinfo.pubplace", "string", "idinfo.citation.citeinfo.pubinfo.pubplace", "string"), 
              ("idinfo.citation.citeinfo.title", "string", "idinfo.citation.citeinfo.title", "string"), 
              ("idinfo.descript.abstract", "string", "idinfo.descript.abstract", "string"), 
              ("idinfo.descript.purpose", "string", "idinfo.descript.purpose", "string"), 
              ("idinfo.descript.supplinf", "string", "idinfo.descript.supplinf", "string"), 
              ("idinfo.keywords.place.placekey", "array", "idinfo.keywords.place.placekey", "array"), 
              ("idinfo.keywords.place.placekt", "string", "idinfo.keywords.place.placekt", "string"), 
              ("idinfo.keywords.theme", "array", "idinfo.keywords.theme", "array"), 
              ("idinfo.ptcontac.cntinfo.cntaddr.address", "string", "idinfo.ptcontac.cntinfo.cntaddr.address", "string"), 
              ("idinfo.ptcontac.cntinfo.cntaddr.addrtype", "string", "idinfo.ptcontac.cntinfo.cntaddr.addrtype", "string"), 
              ("idinfo.ptcontac.cntinfo.cntaddr.city", "string", "idinfo.ptcontac.cntinfo.cntaddr.city", "string"), 
              ("idinfo.ptcontac.cntinfo.cntaddr.postal", "int", "idinfo.ptcontac.cntinfo.cntaddr.postal", "int"), 
              ("idinfo.ptcontac.cntinfo.cntaddr.state", "string", "idinfo.ptcontac.cntinfo.cntaddr.state", "string"), 
              ("idinfo.ptcontac.cntinfo.cntemail", "string", "idinfo.ptcontac.cntinfo.cntemail", "string"), 
              ("idinfo.ptcontac.cntinfo.cntinst", "string", "idinfo.ptcontac.cntinfo.cntinst", "string"), 
              ("idinfo.ptcontac.cntinfo.cntorgp.cntorg", "string", "idinfo.ptcontac.cntinfo.cntorgp.cntorg", "string"), 
              ("idinfo.ptcontac.cntinfo.cntvoice", "string", "idinfo.ptcontac.cntinfo.cntvoice", "string"), 
              ("idinfo.ptcontac.cntinfo.hours", "string", "idinfo.ptcontac.cntinfo.hours", "string"), 
              ("idinfo.spdom.bounding.eastbc", "double", "idinfo.spdom.bounding.eastbc", "double"), 
              ("idinfo.spdom.bounding.northbc", "double", "idinfo.spdom.bounding.northbc", "double"), 
              ("idinfo.spdom.bounding.southbc", "double", "idinfo.spdom.bounding.southbc", "double"), 
              ("idinfo.spdom.bounding.westbc", "double", "idinfo.spdom.bounding.westbc", "double"), 
              ("idinfo.status.progress", "string", "idinfo.status.progress", "string"), 
              ("idinfo.status.update", "string", "idinfo.status.update", "string"), 
              ("idinfo.timeperd.current", "string", "idinfo.timeperd.current", "string"), 
              ("idinfo.timeperd.timeinfo.rngdates.begdate", "int", "idinfo.timeperd.timeinfo.rngdates.begdate", "int"), 
              ("idinfo.timeperd.timeinfo.rngdates.enddate", "string", "idinfo.timeperd.timeinfo.rngdates.enddate", "string"), 
              ("idinfo.useconst", "string", "idinfo.useconst", "string"), 
              ("metainfo.metc.cntinfo.cntaddr", "array", "metainfo.metc.cntinfo.cntaddr", "array"), 
              ("metainfo.metc.cntinfo.cntemail", "string", "metainfo.metc.cntinfo.cntemail", "string"), 
              ("metainfo.metc.cntinfo.cntinst", "string", "metainfo.metc.cntinfo.cntinst", "string"), 
              ("metainfo.metc.cntinfo.cntorgp.cntorg", "string", "metainfo.metc.cntinfo.cntorgp.cntorg", "string"), 
              ("metainfo.metc.cntinfo.cntorgp.cntper", "string", "metainfo.metc.cntinfo.cntorgp.cntper", "string"), 
              ("metainfo.metc.cntinfo.cntvoice", "string", "metainfo.metc.cntinfo.cntvoice", "string"), 
              ("metainfo.metc.cntinfo.hours", "string", "metainfo.metc.cntinfo.hours", "string"), 
              ("metainfo.metd", "int", "metainfo.metd", "int"), 
              ("metainfo.metstdn", "string", "metainfo.metstdn", "string"), 
              ("metainfo.metstdv", "string", "metainfo.metstdv", "string"), 
              ("spdoinfo.direct", "string", "spdoinfo.direct", "string")],    
    transformation_ctx="ApplyMapping_node1744188425972")

# Script generated for node output
EvaluateDataQuality().process_rows(
    frame=ApplyMapping_node1744188425972, 
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node1744187297965", 
        "enableDataQualityResultsPublishing": True}, 
        additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})

output_node1744188511142 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node1744188425972, 
    connection_type="s3", format="glueparquet", 
    connection_options={"path": "s3://acoe-silver-layer/test_intern/xml-files/", "partitionKeys": []}, 
    format_options={"compression": "uncompressed"}, transformation_ctx="output_node1744188511142")

job.commit()