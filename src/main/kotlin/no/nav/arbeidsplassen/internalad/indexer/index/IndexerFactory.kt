package no.nav.arbeidsplassen.internalad.indexer.index

import com.fasterxml.jackson.databind.ObjectMapper
import io.micronaut.context.annotation.Factory
import net.javacrumbs.shedlock.core.LockProvider
import org.elasticsearch.client.RestHighLevelClient
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import javax.inject.Singleton

@Factory
class IndexerFactory(val highLevelClient: RestHighLevelClient, val objectMapper: ObjectMapper) {

    @Singleton
    fun lockProvider(): LockProvider {
        return ElasticsearchLockProvider(highLevelClient=highLevelClient, objectMapper = objectMapper);
    }
}

const val INTERNALAD = "internalad"
val datePattern: DateTimeFormatter = DateTimeFormatter.ofPattern("_yyyyMMdd_HHmmss")

fun internalAdIndexWithTimestamp(): String {
    return INTERNALAD +LocalDateTime.now().format(datePattern)
}


const val INTERNALAD_MAPPING = """{
  "date_detection": false,

  "properties": {
    "id": {
      "type": "keyword",
      "normalizer": "trim_normalizer"
    },
    "uuid": {
      "type": "keyword"
    },
    "createdBy": {
      "type": "keyword"
    },
    "updatedBy": {
      "type": "keyword"
    },
    "updated": {
      "type": "date",
      "format": "strict_date_optional_time"
    },
    "created": {
      "type": "date",
      "format": "strict_date_optional_time"
    },
    "published": {
      "type": "date",
      "format": "strict_date_optional_time"
    },
    "publishedByAdmin": {
      "type": "date",
      "format": "strict_date_optional_time"
    },
    "expires": {
      "type": "date",
      "format": "strict_date_optional_time"
    },
    "status": {
      "type": "keyword"
    },
    "privacy": {
      "type": "keyword"
    },
    "source": {
      "type": "keyword"
    },
    "reference": {
      "type": "keyword"
    },
    "medium": {
      "type": "keyword"
    },
    "title": {
      "type": "text",
      "copy_to": "title_no"
    },
    "businessName": {
      "type": "text",
      "copy_to": "employername"
    },
    "title_no": {
      "type": "text",
      "analyzer": "norwegian"
    },
    "adtext_no": {
      "type": "text",
      "analyzer": "norwegian_html"
    },
    "employername": {
      "type": "text",
      "position_increment_gap": 100
    },
    "employerdescription_no": {
      "type": "text",
      "analyzer": "norwegian_html"
    },
    "geography_all_no": {
      "type": "text",
      "analyzer": "norwegian"
    },

    "engagementtype_facet": {
      "type": "keyword"
    },
    "extent_facet": {
      "type": "keyword"
    },
    "sector_facet": {
      "type": "keyword"
    },
    "county_facet": {
      "type": "keyword"
    },
    "country_facet": {
      "type": "keyword"
    },
    "municipal_facet": {
      "type": "keyword"
    },

    "geopoint": {
      "type": "geo_point"
    },

    "category_no": {
      "type": "text",
      "analyzer": "norwegian"
    },
    "category_description_no": {
      "type": "text",
      "analyzer": "norwegian"
    },
    "category_suggest": {
      "type": "completion",
      "contexts": [
        {
          "name": "status",
          "type": "category",
          "path": "status"
        }
      ]
    },
    "category_styrk08_facet": {
      "type": "keyword"
    },
    "searchtags_no": {
      "type": "text",
      "analyzer": "norwegian"
    },
    "searchtags_suggest": {
      "type": "completion",
      "contexts": [
        {
          "name": "status",
          "type": "category",
          "path": "status"
        }
      ]
    },
    "searchtags_facet": {
      "type": "keyword"
    },

    "occupationList": {
      "type": "nested",
      "properties": {
        "level1": {
          "type": "keyword",
          "copy_to": ["category_suggest", "category_no"]
        },
        "level2": {
          "type": "keyword",
          "copy_to": ["category_suggest", "category_no"]
        }
      }
    },

    "locationList": {
      "type": "nested",
      "properties": {
        "address": {
          "type": "text"
        },
        "city": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword"
            }
          }
        },
        "country": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword"
            }
          },
          "copy_to": ["geography_all_no"]
        },
        "county": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword"
            }
          },
          "copy_to": ["geography_all_no"]
        },
        "municipal": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword"
            }
          },
          "copy_to": ["geography_all_no"]
        },
        "postalCode": {
          "type": "keyword"
        },
        "latitude": {
          "type": "keyword",
          "index": false,
          "doc_values": false
        },
        "longitude": {
          "type": "keyword",
          "index": false,
          "doc_values": false
        }
      }
    },

    "location": {
      "properties": {
        "address": {
          "type": "text"
        },
        "city": {
          "type": "text"
        },
        "country": {
          "type": "text",
          "copy_to": ["geography_all_no"]
        },
        "county": {
          "type": "text",
          "copy_to": ["county_facet", "geography_all_no"]
        },
        "municipal": {
          "type": "text",
          "copy_to": ["municipal_facet", "geography_all_no"]
        },
        "postalCode": {
          "type": "keyword"
        },
        "latitude": {
          "type": "keyword",
          "index": false,
          "doc_values": false
        },
        "longitude": {
          "type": "keyword",
          "index": false,
          "doc_values": false
        }
      }
    },
    "categoryList": {
      "properties": {
        "id": {
          "type": "long"
        },
        "code": {
          "type": "keyword"
        },
        "categoryType": {
          "type": "keyword"
        },
        "name": {
          "type": "text",
          "copy_to": ["category_styrk08_facet", "category_no", "category_suggest"]
        },
        "description": {
          "type": "text",
          "copy_to": "category_description_no"
        },
        "parentId": {
          "type": "long"
        }
      }
    },
    "contactList": {
      "properties": {
        "name": {
          "type": "text"
        },
        "title": {
          "type": "text"
        },
        "email": {
          "type": "keyword"
        },
        "phone": {
          "type": "keyword"
        },
        "role": {
          "type": "keyword"
        }
      }
    },
    "mediaList": {
      "enabled": false
    },
    "employer": {
      "properties": {
        "id": {
          "enabled": false
        },
        "uuid": {
          "enabled": false
        },
        "createdBy": {
          "enabled": false
        },
        "updatedBy": {
          "enabled": false
        },
        "created": {
          "enabled": false
        },
        "updated": {
          "enabled": false
        },
        "contactList": {
          "enabled": false
        },
        "mediaList": {
          "enabled": false
        },
        "location": {
          "enabled": false
        },
        "locationList": {
          "enabled": false
        },
        "name": {
          "type": "text",
          "copy_to": "employername"
        },
        "orgnr": {
          "type": "keyword"
        },
        "status": {
          "type": "keyword"
        },
        "parentOrgnr": {
          "type": "keyword"
        },
        "publicName": {
          "type": "text"
        },
        "deactivated": {
          "type": "date",
          "format": "strict_date_optional_time"
        },
        "orgform": {
          "type": "keyword"
        },
        "employees": {
          "type": "long"
        },
        "properties.nace2.code":{
          "type": "keyword"
        },
        "properties.nace2.name":{
          "type": "text"
        }
      }
    },
    "properties": {
      "properties": {
        "adtext": {
          "type": "text",
          "index": false,
          "copy_to": "adtext_no"
        },
        "employerdescription": {
          "type": "text",
          "index": false,
          "copy_to": "employerdescription_no"
        },
        "duration": {
          "type": "keyword"
        },
        "sourceurl": {
          "type": "keyword"
        },
        "sourcecreated": {
          "type": "keyword"
        },
        "sourceupdated": {
          "type": "keyword"
        },
        "author": {
          "type": "keyword"
        },
        "employer": {
          "type": "text",
          "copy_to": "employername"
        },
        "industry": {
          "type": "text"
        },
        "jobtitle": {
          "type": "text",
          "copy_to": "searchtags_no"
        },
        "location": {
          "type": "text",
          "copy_to": "geography_all_no"
        },
        "sector": {
          "type": "keyword",
          "copy_to": "sector_facet"
        },
        "starttime": {
          "type": "text"
        },
        "applicationdue": {
          "type": "keyword"
        },
        "extent": {
          "type": "keyword",
          "copy_to": "extent_facet"
        },
        "engagementtype": {
          "type": "keyword",
          "copy_to": "engagementtype_facet"
        },
        "positioncount": {
          "type": "integer",
          "ignore_malformed": true
        },
        "searchtags": {
          "properties": {
            "label": {
              "type": "text",
              "copy_to": ["searchtags_no", "searchtags_suggest", "searchtags_facet"]
            },
            "score": {
              "type": "float"
            }
          }
        },
        "classification_styrk08_score": {
          "type": "float"
        },
        "classification_input_source": {
          "type": "keyword"
        }
      }
    }
  }
}
"""

const val INTERNALAD_COMMON_SETTINGS="""{
  "settings": {
    "index": {
      "number_of_shards": 3,
      "number_of_replicas": 2
    },
    "analysis": {
      "filter": {
        "norwegian_stop": {
          "type":       "stop",
          "stopwords":  "_norwegian_"
        },
        "norwegian_stemmer": {
          "type":       "stemmer",
          "language":   "norwegian"
        }
      },
      "char_filter": {
        "custom_trim": {
          "type": "pattern_replace",
          "pattern": "^\\s+|\\s+${'$'}",
          "replacement": ""
        }
      },
      "normalizer": {
        "trim_normalizer": {
          "type": "custom",
          "char_filter": ["custom_trim"]
        },
        "lowercase_normalizer": {
          "type": "custom",
          "char_filter": ["custom_trim"],
          "filter": ["lowercase"]
        },
        "lowercase_folding_normalizer": {
          "type": "custom",
          "char_filter": ["custom_trim"],
          "filter": ["lowercase" ,"asciifolding"]
        }
      },
      "analyzer": {
        "norwegian_html": {
          "tokenizer":  "standard",
          "filter": [
            "lowercase",
            "norwegian_stop",
            "norwegian_stemmer"
          ],
          "char_filter": [
            "html_strip"
          ]
        }
      }
    }
  }
}
"""

