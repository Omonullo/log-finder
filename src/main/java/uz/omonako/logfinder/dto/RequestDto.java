package org.example;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.LinkedList;

@Data
@NoArgsConstructor
public class RequestDto {
  private String key;
  private LinkedList<String> requiredProperties = new LinkedList<>();
  private LinkedList<QueryDto> queries = new LinkedList<>();
}
