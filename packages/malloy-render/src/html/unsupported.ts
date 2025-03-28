/*
 * Copyright 2023 Google LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files
 * (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge,
 * publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

import {Renderer} from './renderer';
import {createNullElement} from './utils';
import {RendererFactory} from './renderer_factory';
import {DataRenderOptions, StyleDefaults} from './data_styles';
import {RendererOptions} from './renderer_types';
import {Cell, Field} from '../data_tree';

export class HTMLUnsupportedRenderer implements Renderer {
  constructor(private readonly document: Document) {}

  getText(data: Cell): string | null {
    if (data.isString() || data.isNull()) {
      return data.value;
    }
    const value = data.value;
    if (
      typeof value === 'string' ||
      typeof value === 'number' ||
      typeof value === 'boolean'
    ) {
      return value.toString();
    }
    return JSON.stringify(value);
  }

  async render(data: Cell): Promise<HTMLElement> {
    const text = this.getText(data);
    if (text === null) {
      return createNullElement(this.document);
    }

    const element = this.document.createElement('span');
    element.appendChild(this.document.createTextNode(text));
    return element;
  }
}

export class UnsupportedRendererFactory extends RendererFactory<DataRenderOptions> {
  public static readonly instance = new UnsupportedRendererFactory();

  activates(field: Field): boolean {
    return field.isSQLNative();
  }

  create(
    document: Document,
    _styleDefaults: StyleDefaults,
    _rendererOptions: RendererOptions,
    _field: Field,
    _options: DataRenderOptions
  ): Renderer {
    return new HTMLUnsupportedRenderer(document);
  }

  get rendererName() {
    return 'unsupported';
  }
}
