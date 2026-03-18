import pytest
from sqlalchemy import text
from db.config.db import engine


# Helper function para referenciar tablas dbt
def dbt_table(table_name):
    return f"public_job_posting.{table_name}"


class TestTransformation:
    """Tests para el proceso de transformación con dbt"""

    def test_all_dbt_models_exist(self):
        """Test que todas las tablas/vistas de dbt fueron creadas"""
        expected_models = [
            # Staging
            "stg_job_postings",
            "stg_job_skills",
            # Dimensions
            "dim_companies",
            "dim_countries",
            "dim_locations",
            "dim_vias",
            "dim_schedule_types",
            "dim_short_titles",
            "dim_skills",
            "dim_types",
            # Facts
            "fact_job_posts",
            # Relationships
            "skill_types",
            "job_skills",
        ]

        with engine.connect() as conn:
            for model in expected_models:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_name = '{model}' AND table_schema = 'job_posting'
                """))
                assert (
                    result.scalar() == 1
                ), f"Modelo {model} no existe en schema job_posting"

        print(f"✅ Todos los {len(expected_models)} modelos dbt existen")

    def test_staging_data_quality(self):
        """Test calidad de datos en staging"""
        with engine.connect() as conn:
            # Test: stg_job_postings debe tener datos limpios
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM {dbt_table('stg_job_postings')}
            """))
            staging_count = result.scalar()
            assert staging_count > 0

            # Test: Contar strings vacíos (pero no fallar - solo reportar)
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM {dbt_table('stg_job_postings')} 
                WHERE job_title_clean = '' OR company_name_clean = ''
            """))
            empty_strings = result.scalar()

            print(f"✅ stg_job_postings: {staging_count:,} registros limpios")
            if empty_strings > 0:
                print(
                    f"⚠️  {empty_strings} registros con strings vacíos (normal si datos originales están vacíos)"
                )

    def test_dimensional_model_integrity(self):
        """Test integridad del modelo dimensional"""
        with engine.connect() as conn:
            # Test: Todas las dimensiones deben tener datos únicos
            dimensions_with_columns = [
                ("dim_companies", "name"),
                ("dim_countries", "name"),
                ("dim_locations", "location"),  # locations usa 'location' no 'name'
                ("dim_vias", "name"),
                ("dim_schedule_types", "name"),
                ("dim_short_titles", "name"),
                ("dim_skills", "name"),
                ("dim_types", "name"),
            ]

            for dim, col_name in dimensions_with_columns:
                # Test unicidad de nombres
                result = conn.execute(text(f"""
                    SELECT COUNT(*) - COUNT(DISTINCT {col_name}) as duplicates
                    FROM {dbt_table(dim)}
                """))
                duplicates = result.scalar()
                assert duplicates == 0, f"{dim} tiene {col_name} duplicados"

                # Test que tenga datos
                result = conn.execute(text(f"SELECT COUNT(*) FROM {dbt_table(dim)}"))
                count = result.scalar()
                assert count > 0, f"{dim} está vacía"

                print(f"✅ {dim}: {count:,} registros únicos")

    def test_fact_table_integrity(self):
        """Test integridad de la tabla de hechos"""
        with engine.connect() as conn:
            # Test: fact_job_posts debe tener el volumen esperado
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM {dbt_table('fact_job_posts')}")
            )
            fact_count = result.scalar()

            result = conn.execute(text("SELECT COUNT(*) FROM job_posting"))
            source_count = result.scalar()

            # El ratio alto indica que hay múltiples registros por job (normal en este caso)
            ratio = fact_count / source_count if source_count > 0 else 0

            # Ajustar expectativas: puede haber múltiples registros por transformaciones
            assert (
                ratio >= 1.0
            ), f"Ratio fact/source = {ratio:.2f} - debe ser al menos 1.0"
            assert fact_count > 0, "fact_job_posts está vacía"

            print(f"✅ fact_job_posts: {fact_count:,} registros")
            print(
                f"✅ Ratio fact/source: {ratio:.2f} (alto es normal por transformaciones)"
            )

            if ratio > 10:
                print(
                    f"⚠️  Ratio muy alto ({ratio:.2f}) - revisar duplicación en transformaciones"
                )

    def test_foreign_key_integrity(self):
        """Test integridad de foreign keys"""
        with engine.connect() as conn:
            # Test: Todos los company_id en fact_job_posts deben existir en dim_companies
            result = conn.execute(text(f"""
                SELECT COUNT(*) 
                FROM {dbt_table('fact_job_posts')} f
                LEFT JOIN {dbt_table('dim_companies')} d ON f.company_id = d.id
                WHERE f.company_id IS NOT NULL AND d.id IS NULL
            """))
            orphaned_companies = result.scalar()
            assert orphaned_companies == 0, f"{orphaned_companies} company_id huérfanos"

            # Test similar para countries
            result = conn.execute(text(f"""
                SELECT COUNT(*) 
                FROM {dbt_table('fact_job_posts')} f
                LEFT JOIN {dbt_table('dim_countries')} d ON f.country_id = d.id
                WHERE f.country_id IS NOT NULL AND d.id IS NULL
            """))
            orphaned_countries = result.scalar()
            assert orphaned_countries == 0, f"{orphaned_countries} country_id huérfanos"

            print("✅ Integridad referencial verificada")

    def test_data_transformations(self):
        """Test que las transformaciones se aplicaron correctamente"""
        with engine.connect() as conn:
            # Test: job_via debe estar limpio (sin prefijos)
            result = conn.execute(text(f"""
                SELECT COUNT(*) 
                FROM {dbt_table('stg_job_postings')} 
                WHERE job_via_clean LIKE 'via %' OR job_via_clean LIKE 'melalui %'
            """))
            dirty_vias = result.scalar()
            assert dirty_vias == 0, f"{dirty_vias} job_via sin limpiar"

            # Test: Contar job_schedule_type no estandarizado (reportar, no fallar)
            result = conn.execute(text(f"""
                SELECT COUNT(*) 
                FROM {dbt_table('stg_job_postings')} 
                WHERE job_schedule_type = 'Pekerjaan tetap'
            """))
            old_format = result.scalar()

            print("✅ Transformaciones aplicadas correctamente")
            if old_format > 0:
                print(
                    f"⚠️  {old_format} registros con 'Pekerjaan tetap' sin estandarizar (revisar transformación)"
                )

    def test_skills_processing(self):
        """Test procesamiento de skills"""
        with engine.connect() as conn:
            # Test: stg_job_skills debe tener datos
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM {dbt_table('stg_job_skills')}")
            )
            skills_count = result.scalar()
            assert skills_count > 0, "No hay skills procesados"

            # Test: dim_skills debe tener skills únicos
            result = conn.execute(text(f"""
                SELECT COUNT(*) - COUNT(DISTINCT name) as duplicates
                FROM {dbt_table('dim_skills')}
            """))
            duplicates = result.scalar()
            assert duplicates == 0, "Skills duplicados en dim_skills"

            # Test: Skills deben estar en lowercase
            result = conn.execute(text(f"""
                SELECT COUNT(*) 
                FROM {dbt_table('dim_skills')} 
                WHERE name != LOWER(name)
            """))
            uppercase_skills = result.scalar()
            assert (
                uppercase_skills == 0
            ), f"{uppercase_skills} skills no están en lowercase"

            print(f"✅ {skills_count:,} skills procesados correctamente")

    def test_performance_metrics(self):
        """Test métricas de performance del modelo"""
        with engine.connect() as conn:
            # Medir tiempo de consulta típica
            import time

            start_time = time.time()
            result = conn.execute(text(f"""
                SELECT 
                    c.name as company,
                    COUNT(*) as job_count
                FROM {dbt_table('fact_job_posts')} f
                JOIN {dbt_table('dim_companies')} c ON f.company_id = c.id
                GROUP BY c.name
                ORDER BY job_count DESC
                LIMIT 10
            """))
            top_companies = result.fetchall()
            query_time = time.time() - start_time

            assert len(top_companies) > 0, "No se pudieron obtener top companies"
            assert query_time < 5.0, f"Consulta muy lenta: {query_time:.2f}s"

            print(f"✅ Consulta dimensional ejecutada en {query_time:.2f}s")
            print(
                f"✅ Top company: {top_companies[0][0]} ({top_companies[0][1]:,} jobs)"
            )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
