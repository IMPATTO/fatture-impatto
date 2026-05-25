export const VILLA_MARGHERITA_BASE_URL = 'https://villamargheritarimini.com';

export const VILLA_MARGHERITA_PAGE_PATHS = Object.freeze([
  '/',
  '/monolocali-rimini.html',
  '/bilocali-rimini.html',
  '/trilocali-rimini.html',
  '/residence-rimini-terme.html',
  '/offerte-vacanze-rimini.html',
  '/regolamento-residence-margherita.html',
]);

export function getVillaMargheritaUrls() {
  return VILLA_MARGHERITA_PAGE_PATHS.map((pagePath) => new URL(pagePath, `${VILLA_MARGHERITA_BASE_URL}/`).toString());
}
