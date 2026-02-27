#!/bin/bash
echo "--- Terminal 1: Banco de Dados Neo4j ---"
echo "Tentando subir o banco de dados..."
# Tenta rodar com sudo se o comando normal falhar por permissão
if ! docker-compose up; then
    echo "Falhou. Tentando com sudo (pode pedir sua senha)..."
    sudo systemctl start docker
    sudo docker-compose up
fi
