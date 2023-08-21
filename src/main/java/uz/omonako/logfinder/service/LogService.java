package uz.omonako.logfinder.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import uz.omonako.logfinder.dto.QueryDto;
import uz.omonako.logfinder.dto.RequestDto;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class LogService {

    private final JdbcTemplate jdbcTemplate;

    @Autowired
    private RestHighLevelClient restHighLevelClient;


    public LogService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    private HashMap<String, String> getKeyVMap(List<String> parameterList) {
        String sqlQuery = "select coalesce(so.client_uid, c.client_uid, p.phone) as keyc, coalesce(c.pinfl, p.pinfl) as valuec\n" +
                "from claim c\n" +
                "         right join scoring.person p on p.pinfl = c.pinfl\n" +
                "         right join scoring.order so on so.id = p.order_id\n" +
                "where p.phone in (?);";
        String parameters = String.join(",", parameterList);  // Convert the list to a comma-separated string
        String formattedQuery = sqlQuery.replace("?", parameters);

        List<Map<String, Object>> resultSet = jdbcTemplate.queryForList(formattedQuery);

        HashMap<String, String> resultMap = new HashMap<>();
        for (Map<String, Object> row : resultSet) {
            String key = (String) row.get("keyc");
            String value = (String) row.get("valuec");
            resultMap.put(key, value);
        }

        return resultMap;
    }
    public String searchUpdateQueries(List<String> pinflList) {
        HashMap<String, String> kvMap = getKeyVMap(pinflList);
        StringBuilder resultSB = new StringBuilder();
        try {
            String indexName = "solfy-esb-logs-prod-*";
            kvMap.keySet().forEach(key -> {
                final RequestDto reqRequest = new RequestDto();

                LinkedList<QueryDto> queries = new LinkedList<>();
                queries.add(new QueryDto("iHdrs.App", "=", "Adp.Prx.WS.UpdateCustomer.V1"));
                queries.add(new QueryDto("message", "=", key));
                queries.add(new QueryDto("iHdrs.Pnt", "=", "REQ START"));
                reqRequest.setQueries(queries);
                LinkedList<String> requiredProperties = new LinkedList<>();
                requiredProperties.add("Content.json");
                requiredProperties.add("@timestamp");
                String iHdrsIID = "iHdrs.IId";
                requiredProperties.add(iHdrsIID);
                reqRequest.setRequiredProperties(requiredProperties);
                reqRequest.setKey(kvMap.get(key));

                Optional<Map<String, String>> result = extractAResult(restHighLevelClient, indexName, reqRequest);
                if (result.isPresent()) {
                    Map<String, String> reqValues = result.get();

                    final RequestDto respRequest = new RequestDto();

                    LinkedList<QueryDto> respQueries = new LinkedList<>();
                    respQueries.add(new QueryDto("iHdrs.App", "=", "Adp.Prx.WS.UpdateCustomer.V1"));
                    respQueries.add(new QueryDto("iHdrs.IId", "=", reqValues.get(iHdrsIID).replace("\"", "").split("-")[0]));
                    queries.add(new QueryDto("iHdrs.Pnt", ":", "RESP START"));

                    respRequest.setQueries(respQueries);
                    LinkedList<String> respRequiredProperties = new LinkedList<>();
                    respRequiredProperties.add("Content");
                    respRequiredProperties.add(iHdrsIID);
                    respRequiredProperties.add("@timestamp");
                    respRequest.setRequiredProperties(respRequiredProperties);
                    respRequest.setKey(reqValues.get(iHdrsIID));

                    Optional<Map<String, String>> respResult = extractAResult(restHighLevelClient, indexName, respRequest);
                    Map<String, String> respValues = respResult.stream().findFirst().orElse(new HashMap<>());
                    StringBuilder sb = new StringBuilder();
                    sb.append("|||")
                            .append("\n")
                            .append("pinfl: ").append(kvMap.get(key))
                            .append("\n")
                            .append("timestamp: ").append(reqValues.get("@timestamp"))
                            .append("\n")
                            .append("iHdrs.IId: ").append(reqValues.get(iHdrsIID))
                            .append("\n")
                            .append("REQ: ").append(reqValues.get("Content.json"))
                            .append("\n")
                            .append("RESP ").append(respValues.get("Content"))
                            .append("\n")
                            .append("valid: ").append(" ").append(reqValues.get(iHdrsIID).equals(respValues.get(iHdrsIID)));
                    resultSB.append(sb).append("\n");
                } else {
                    resultSB.append("|||").append("\n");
                    resultSB.append(kvMap.get(key) + " NOT FOUND VALUES").append("\n");
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultSB.toString();
    }

    private Optional<Map<String, String>> extractAResult(RestHighLevelClient client, String indexName, RequestDto requestDto) {
        try {
            // Create a search request
            SearchRequest searchRequest = new SearchRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.sort("@timestamp", SortOrder.DESC);
            final BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            requestDto.getQueries().forEach(query -> boolQueryBuilder.must(convertToQueryBuilder(query.getField(), query.getOperator(), query.getValue())));
            searchSourceBuilder.query(boolQueryBuilder);
            searchRequest.source(searchSourceBuilder);

            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            SearchHit[] searchHits = searchResponse.getHits().getHits();
            Stream<SearchHit> logEntries = Arrays.stream(searchHits);
            List<Map<String, String>> collect = logEntries
                    .map(SearchHit::getSourceAsString)
                    .map(value -> extractJsonProperty(value, requestDto.getRequiredProperties()))
                    .collect(Collectors.toList());
            Optional<Map<String, String>> result = collect.stream().findFirst();
            return result;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    private Map<String, String> extractJsonProperty(String jsonString, List<String> propertyNameList) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            final JsonNode jsonNode = objectMapper.readTree(jsonString);
            return propertyNameList.stream()
                    .map(item -> item.split("\\."))
                    .map(arr -> {
                        return getStringStringEntry(jsonNode, arr);
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new HashMap<>();
    }

    private Map.Entry<String, String> getStringStringEntry(JsonNode jsonNode, String[] arr) {
        JsonNode currentNode = jsonNode;
        for (String prop : arr) {
            currentNode = currentNode.get(prop);
            if (currentNode == null) return null;
        }
        return new AbstractMap.SimpleEntry<>(String.join(".", arr), currentNode.toString());
    }

    private QueryBuilder convertToQueryBuilder(String field, String operator, String value) {
        switch (operator) {
            case "=":
                return QueryBuilders.matchQuery(field, value);
            case ":":
                return QueryBuilders.termQuery(field, value);
            case "!=":
                return QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(field, value));
            default:
                throw new IllegalArgumentException("Invalid operator: " + operator);
        }
    }

    private Long parseToTimeInMillis(String time) {
        SimpleDateFormat format = new SimpleDateFormat(
                "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
        try {

            Date parsedDate = format.parse(time);
            return parsedDate.getTime();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
