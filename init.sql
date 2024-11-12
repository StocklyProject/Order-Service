CREATE DATABASE IF NOT EXISTS stockly;
USE stockly;

CREATE TABLE IF NOT EXISTS company (
  id INT AUTO_INCREMENT PRIMARY KEY,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  is_deleted TINYINT(1) DEFAULT 0,
  name VARCHAR(100) NOT NULL,
  symbol VARCHAR(100) NOT NULL
);

INSERT INTO company (name, symbol) VALUES
('삼성전자', '005930'),
('삼성전자우', '005935'),
('SK하이닉스', '000660'),
('LG엔솔', '373220'),
('현대차', '005380'),
('현대차3우B', '005389'),
('현대차우', '005385'),
('현대차2우B', '005387'),
('삼성바이오로직스', '207940'),
('기아', '000270'),
('셀트리온', '068270'),
('LG화학', '051910'),
('POSCO홀딩스', '005490'),
('LG화학우', '051915'),
('NAVER', '035420'),
('삼성SDI', '006400'),
('삼성SDI우', '006405'),
('KB금융', '105560'),
('삼성물산', '028260');