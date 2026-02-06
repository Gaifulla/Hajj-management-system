package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/xuri/excelize/v2"
)

func main() {
	// 1. ПОДКЛЮЧЕНИЕ (Порт 5433)
	connStr := "postgres://user:password@localhost:5433/hajj_db"
	conn, err := pgx.Connect(context.Background(), connStr)
	if err != nil {
		log.Fatalf("Ошибка подключения: %v\n", err)
	}
	defer conn.Close(context.Background())

	// 2. ОТКРЫТИЕ ФАЙЛА
	f, err := excelize.OpenFile("Test Planning.xlsx")
	if err != nil {
		log.Fatalf("Файл не найден! Ошибка: %v\n", err)
	}
	defer f.Close()

	rows, err := f.GetRows("PLANNING")
	if err != nil || len(rows) == 0 {
		log.Fatalf("Лист PLANNING пуст или не найден\n")
	}

	// 3. ФИЛЬТРАЦИЯ: Оставляем только заполненные колонки
	rawHeaders := rows[0]
	var cleanHeaders []string    // Имена для SQL
	var validColumnIndices []int // Индексы (0, 1, 2...) только полезных колонок
	usedNames := make(map[string]int)

	for i, h := range rawHeaders {
		name := strings.ToLower(strings.TrimSpace(h))

		// ГЛАВНОЕ ПРАВИЛО: Если имя пустое или это ошибка #REF! — ИГНОРИРУЕМ
		if name == "" || strings.Contains(name, "ref") {
			continue
		}

		// Если имя состоит только из цифр
		// Мы превращаем её в "col_1"
		if isNumeric(name) {
			name = "col_" + name
		}

		// Чистим символы
		name = strings.ReplaceAll(name, " ", "_")
		name = strings.ReplaceAll(name, "№", "no")
		name = strings.ReplaceAll(name, ".", "")

		// Если имя повторяется
		usedNames[name]++
		if usedNames[name] > 1 {
			name = fmt.Sprintf("%s_%d", name, usedNames[name])
		}

		cleanHeaders = append(cleanHeaders, name)
		validColumnIndices = append(validColumnIndices, i) // Запоминаем, что эту колонку надо брать
	}

	// 4. СОЗДАНИЕ ТАБЛИЦЫ
	conn.Exec(context.Background(), "DROP TABLE IF EXISTS dynamic_tours")

	var columnDefs []string
	for _, h := range cleanHeaders {
		columnDefs = append(columnDefs, fmt.Sprintf("%s TEXT", h))
	}

	createTableSQL := fmt.Sprintf("CREATE TABLE dynamic_tours (id SERIAL PRIMARY KEY, %s);",
		strings.Join(columnDefs, ", "))

	_, err = conn.Exec(context.Background(), createTableSQL)
	if err != nil {
		log.Fatalf("Ошибка при создании таблицы: %v\nSQL: %s", err, createTableSQL)
	}
	fmt.Println("Таблица создана! Колонки:", strings.Join(cleanHeaders, ", "))

	// 5. ВСТАВКА ДАННЫХ
	for i, row := range rows {
		if i == 0 {
			continue
		} // Пропуск шапки

		placeholders := []string{}
		for j := range cleanHeaders {
			placeholders = append(placeholders, fmt.Sprintf("$%d", j+1))
		}

		insertSQL := fmt.Sprintf("INSERT INTO dynamic_tours (%s) VALUES (%s);",
			strings.Join(cleanHeaders, ", "),
			strings.Join(placeholders, ", "))

		// Берем данные ТОЛЬКО из тех индексов, которые мы признали "валидными"
		values := make([]interface{}, len(validColumnIndices))
		for j, originalIdx := range validColumnIndices {
			if originalIdx < len(row) {
				values[j] = row[originalIdx]
			} else {
				values[j] = ""
			}
		}

		_, err = conn.Exec(context.Background(), insertSQL, values...)
		if err != nil {
			fmt.Printf("Строка %d пропущена: %v\n", i+1, err)
		}
	}

	fmt.Println("Миграция завершена успешно!")
}

// Вспомогательная функция для проверки, является ли строка числом
func isNumeric(s string) bool {
	if len(s) == 0 {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
