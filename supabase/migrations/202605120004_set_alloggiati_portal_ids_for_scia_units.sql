update public.apartment_alloggiati aa
set id_appartamento_portale = mapping.id_portale
from (
  values
    ('Misano Adriatico - Via Litoranea Sud 40 con Balcone', '0'),
    ('Misano Adriatico - Via Litoranea Sud 40 con Terrazza Panoramica', '1'),
    ('Cattolica - Via Fiume 37', '0'),
    ('Cattolica - Via Bovio 3', '1'),
    ('Cattolica - Via Salvator Allende 144', '2'),
    ('Cattolica - Via Rasi Spinelli 2', '3'),
    ('Cattolica - Via Donizetti 13/a', '4')
) as mapping(apartment_name, id_portale)
where aa.apartment_id = (
  select a.id
  from public.apartments a
  where a.nome_appartamento = mapping.apartment_name
  limit 1
);

comment on table public.apartment_alloggiati is
'Collegamento appartamento -> account AlloggiatiWeb. id_appartamento_portale si usa solo per appartamenti gestiti via SCIA aziendale / account Gestore Appartamenti.';
