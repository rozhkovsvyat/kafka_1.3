package com.work.dto;

import java.util.UUID;

/**Avro автоматически построит схему на основе полей этого класса
 * благодаря включенному SCHEMA_REFLECTION_CONFIG в продюсере.*/
public class MessageDto {
    private String id; // Хранит строковое представление UUID.
    private int number;

    public MessageDto() {} // Обязателен для десериализации Avro.

    public MessageDto(UUID id, int number) {
        this.id = id.toString();
        this.number = number;
    }

    public UUID getId() { return UUID.fromString(id); }
    public void setId(UUID id) { this.id = id.toString(); }
    public void setId(String id) { this.id = id; }
    
    public int getNumber() { return number; }
    public void setNumber(int number) { this.number = number; }
}
