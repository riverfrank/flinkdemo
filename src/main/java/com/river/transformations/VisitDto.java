package com.river.transformations;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor

public class VisitDto implements Serializable {
    private Integer id;
    private String name;
    private Integer age;
}
