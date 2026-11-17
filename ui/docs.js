'use strict';
Scalar.createApiReference('#app', {
  url: '/openapi.yml',
  telemetry: false,
  withDefaultFonts: false,
  hideTestRequestButton: true,
  hideClientButton: true,
  documentDownloadType: 'none',
  showDeveloperTools: 'never',
  agent: { disabled: true }
});
