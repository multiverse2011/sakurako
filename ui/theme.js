'use strict';
(() => {
  const key = 'sakurako-theme';
  const system = window.matchMedia('(prefers-color-scheme: dark)');
  let preference;
  try { preference = localStorage.getItem(key); } catch (_) {}
  const apply = () => {
    const dark = preference === 'dark' || (preference !== 'light' && system.matches);
    document.documentElement.dataset.theme = dark ? 'dark' : 'light';
    const toggle = document.getElementById('dark-mode');
    if (toggle) toggle.checked = dark;
  };
  apply();
  document.addEventListener('DOMContentLoaded', () => {
    apply();
    document.getElementById('dark-mode').addEventListener('change', event => {
      preference = event.target.checked ? 'dark' : 'light';
      try { localStorage.setItem(key, preference); } catch (_) {}
      apply();
    });
  });
  system.addEventListener('change', apply);
  window.addEventListener('storage', event => {
    if (event.key === key || event.key === null) {
      preference = event.newValue;
      apply();
    }
  });
})();
